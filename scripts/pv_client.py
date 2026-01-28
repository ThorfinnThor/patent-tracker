from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


DEFAULT_BASE_URL = "https://search.patentsview.org/api/v1"


class PVError(RuntimeError):
    pass


@dataclass
class PVClient:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_s: int = 60

    # The API enforces 45 requests/minute per key. We'll stay comfortably below that. :contentReference[oaicite:4]{index=4}
    min_seconds_between_requests: float = 60.0 / 40.0  # ~1.5s

    _last_request_ts: float = 0.0

    def _sleep_if_needed(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_seconds_between_requests:
            time.sleep(self.min_seconds_between_requests - elapsed)

    def request(
        self,
        endpoint: str,
        q: Dict[str, Any],
        f: Optional[List[str]] = None,
        s: Optional[List[Dict[str, str]]] = None,
        o: Optional[Dict[str, Any]] = None,
        method: str = "GET",
    ) -> Dict[str, Any]:
        """
        Performs a PatentsView PatentSearch API request.
        Query params: q (required), f, s, o. :contentReference[oaicite:5]{index=5}
        """
        if not q:
            raise ValueError("q is required and must be a non-empty dict")

        self._sleep_if_needed()

        url = f"{self.base_url.rstrip('/')}/{endpoint.strip('/')}/"
        headers = {
            "X-Api-Key": self.api_key,
            "Accept": "application/json",
        }

        params: Dict[str, str] = {"q": json.dumps(q, separators=(",", ":"))}
        if f is not None:
            params["f"] = json.dumps(f, separators=(",", ":"))
        if s is not None:
            params["s"] = json.dumps(s, separators=(",", ":"))
        if o is not None:
            params["o"] = json.dumps(o, separators=(",", ":"))

        try:
            if method.upper() == "POST":
                resp = requests.post(url, headers=headers, json=params, timeout=self.timeout_s)
            else:
                resp = requests.get(url, headers=headers, params=params, timeout=self.timeout_s)
        finally:
            self._last_request_ts = time.time()

        if resp.status_code != 200:
            reason = resp.headers.get("X-Status-Reason", "")
            raise PVError(f"HTTP {resp.status_code} {resp.text[:300]} {reason}")

        data = resp.json()
        # 'error' exists in the response schema. :contentReference[oaicite:6]{index=6}
        if str(data.get("error", "false")).lower() == "true":
            raise PVError(f"API returned error=true: {data}")

        return data

    def paginate(
        self,
        endpoint: str,
        q: Dict[str, Any],
        f: List[str],
        s: List[Dict[str, str]],
        size: int = 1000,
        method: str = "GET",
    ) -> Iterable[Dict[str, Any]]:
        """
        Cursor pagination using o.after, which must match sort fields. :contentReference[oaicite:7]{index=7}
        Yields each page's full response.
        """
        after: Optional[Any] = None
        while True:
            o: Dict[str, Any] = {"size": min(size, 1000)}
            if after is not None:
                o["after"] = after

            data = self.request(endpoint=endpoint, q=q, f=f, s=s, o=o, method=method)

            yield data

            total_hits = int(data.get("total_hits", 0))
            count = int(data.get("count", 0))
            if count == 0:
                break

            # Determine response key by endpoint naming convention:
            # e.g., /patent => "patents" per Endpoint Dictionary. :contentReference[oaicite:8]{index=8}
            if endpoint.strip("/") == "patent":
                records = data.get("patents", [])
            elif endpoint.strip("/") == "assignee":
                records = data.get("assignees", [])
            else:
                # fallback: try plural
                records = next((v for k, v in data.items() if isinstance(v, list)), [])
            if not records:
                break

            # Compute next cursor from last record's sort fields.
            last = records[-1]
            after_values: List[Any] = []
            for sort_spec in s:
                field = next(iter(sort_spec.keys()))
                # nested fields not needed for our sorts; but handle dotted paths defensively
                after_values.append(_get_by_dotted_path(last, field))
            after = after_values[0] if len(after_values) == 1 else after_values

            # Stop condition: if we got all hits
            # We can't rely solely on totals because of paging; but this is a helpful guard.
            if count < o["size"]:
                break


def _get_by_dotted_path(obj: Dict[str, Any], path: str) -> Any:
    cur: Any = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur
