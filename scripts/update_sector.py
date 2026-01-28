from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta

from pv_client import PVClient
from normalize import load_assignee_map, map_assignee, normalize_name_for_suggestions


@dataclass(frozen=True)
class SectorConfig:
    sector_id: str  # "biotech" or "tech"
    cpc_subclass_prefixes: List[str]


def _today_iso() -> str:
    return date.today().isoformat()


def _five_years_ago_iso() -> str:
    return (date.today() - relativedelta(years=5)).isoformat()


def load_last_run(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_last_run(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def build_cpc_query(prefixes: List[str]) -> Dict[str, Any]:
    # Filter using CPC subclass IDs with _begins against cpc_current.cpc_subclass_id.
    return {
        "_or": [
            {"_begins": {"cpc_current.cpc_subclass_id": p}}
            for p in prefixes
        ]
    }


def update_sector_pairs(
    client: PVClient,
    sector: SectorConfig,
    assignee_map_path: str,
    last_run_path: str,
    out_pairs_csv_path: str,
) -> None:
    """
    Maintains a 'pairs' table: one row per (patent_id, canonical_company_id, assignee_id).

    Incrementally pulls newly granted patents since last run, filtered by CPC sector.

    IMPORTANT: This uses a 7-day overlap on incremental pulls to avoid missing late-indexed
    records or boundary-date collisions, and relies on dedupe for correctness.
    """

    last_run = load_last_run(last_run_path)
    sector_state = last_run.get(sector.sector_id, {})

    window_start = _five_years_ago_iso()
    today = _today_iso()

    # Incremental start:
    # - If first run: start at 5y window
    # - Else: start at last_success_date
    start_date = sector_state.get("last_success_date") or window_start

    # Clamp to 5y window
    if start_date < window_start:
        start_date = window_start

    # 7-day lookback overlap to avoid missed records
    try:
        d = datetime.strptime(start_date, "%Y-%m-%d").date()
        start_date = (d - timedelta(days=7)).isoformat()
        if start_date < window_start:
            start_date = window_start
    except Exception:
        start_date = window_start

    # Patent endpoint fields we need
    fields = [
        "patent_id",
        "patent_title",
        "patent_date",
        "patent_num_times_cited_by_us_patents",
        "cpc_current.cpc_subclass_id",
        "assignees.assignee_id",
        "assignees.assignee_organization",
        "assignees.assignee_type",
    ]

    # Sort for cursor pagination: patent_date asc, then patent_id asc
    sort = [{"patent_date": "asc"}, {"patent_id": "asc"}]

    q: Dict[str, Any] = {
        "_and": [
            {"patent_type": "utility"},
            {"_gt": {"patent_date": start_date}},
            {"_lte": {"patent_date": today}},
            build_cpc_query(sector.cpc_subclass_prefixes),
        ]
    }

    assignee_map = load_assignee_map(assignee_map_path)

    # Load existing pairs if any
    os.makedirs(os.path.dirname(out_pairs_csv_path), exist_ok=True)
    if os.path.exists(out_pairs_csv_path):
        existing = pd.read_csv(out_pairs_csv_path, dtype=str)
    else:
        existing = pd.DataFrame(columns=[
            "sector_id",
            "patent_id",
            "patent_date",
            "patent_title",
            "patent_num_times_cited_by_us_patents",
            "cpc_subclass_ids",  # pipe-delimited
            "assignee_id",
            "assignee_type",
            "assignee_organization",
            "canonical_company_id",
            "display_name",
        ])

    new_rows: List[Dict[str, Any]] = []
    seen_new_pairs: Set[Tuple[str, str, str]] = set()  # (patent_id, canonical_company_id, assignee_id)

    for page in client.paginate(endpoint="patent", q=q, f=fields, s=sort, size=1000):
        patents = page.get("patents", [])
        if not patents:
            continue

        for p in patents:
            patent_id = str(p.get("patent_id", "")).strip()
            if not patent_id:
                continue

            patent_date = (p.get("patent_date") or "").strip()
            patent_title = (p.get("patent_title") or "").strip()
            cited_by = p.get("patent_num_times_cited_by_us_patents")
            cited_by_str = "" if cited_by is None else str(cited_by)

            # CPC subclasses may appear multiple times; collect distinct.
            cpcs = p.get("cpc_current", []) or []
            cpc_subs = sorted({(c.get("cpc_subclass_id") or "").strip() for c in cpcs if c.get("cpc_subclass_id")})
            cpc_subs_str = "|".join([x for x in cpc_subs if x])

            assignees = p.get("assignees", []) or []
            for a in assignees:
                assignee_id = (a.get("assignee_id") or "").strip()
                if not assignee_id:
                    continue

                assignee_type = (a.get("assignee_type") or "").strip()
                assignee_org = (a.get("assignee_organization") or "").strip()

                canonical_company_id, display_name = map_assignee(
                    assignee_id=assignee_id,
                    raw_org_name=assignee_org,
                    assignee_map=assignee_map,
                )

                key = (patent_id, canonical_company_id, assignee_id)
                if key in seen_new_pairs:
                    continue
                seen_new_pairs.add(key)

                new_rows.append({
                    "sector_id": sector.sector_id,
                    "patent_id": patent_id,
                    "patent_date": patent_date,
                    "patent_title": patent_title,
                    "patent_num_times_cited_by_us_patents": cited_by_str,
                    "cpc_subclass_ids": cpc_subs_str,
                    "assignee_id": assignee_id,
                    "assignee_type": assignee_type,
                    "assignee_organization": assignee_org,
                    "canonical_company_id": canonical_company_id,
                    "display_name": display_name,
                })

    if new_rows:
        new_df = pd.DataFrame(new_rows)

        # Upsert/dedupe on (sector_id, patent_id, canonical_company_id, assignee_id)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.drop_duplicates(
            subset=["sector_id", "patent_id", "canonical_company_id", "assignee_id"],
            inplace=True,
            keep="last",
        )

        # Enforce 5-year window by patent_date (ISO compare is fine for YYYY-MM-DD)
        combined = combined[(combined["patent_date"] >= window_start) & (combined["patent_date"] <= today)]

        combined.to_csv(out_pairs_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # Update state
    last_run[sector.sector_id] = {
        "last_success_date": today,
        "window_start": window_start,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    save_last_run(last_run_path, last_run)


def write_normalization_suggestions(pairs_csv_path: str, out_path: str) -> None:
    """
    Writes a lightweight report of likely name variants to help you update assignee_map.yml.
    This is suggestions only; it does not change canonicalization.
    """
    if not os.path.exists(pairs_csv_path):
        return

    df = pd.read_csv(pairs_csv_path, dtype=str)
    if df.empty:
        return

    # Group by normalized organization strings to flag variant spellings/suffixes.
    df["org_norm"] = df["assignee_organization"].fillna("").map(normalize_name_for_suggestions)

    g = (
        df[df["org_norm"] != ""]
        .groupby("org_norm")
        .agg(
            assignee_ids=("assignee_id", lambda s: sorted(set(s))[:20]),
            org_names=("assignee_organization", lambda s: sorted(set(s))[:20]),
            count=("patent_id", "count"),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )

    # Keep only cases where multiple assignee_ids appear for same normalized name (potential duplicates).
    g["assignee_id_count"] = g["assignee_ids"].map(len)
    g = g[g["assignee_id_count"] >= 2].head(200)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Normalization Suggestions\n\n")
        f.write("These are *potential* duplicates based on normalized organization strings.\n")
        f.write("Review and (optionally) add mappings in data/normalization/assignee_map.yml.\n\n")
        for _, row in g.iterrows():
            f.write(f"## {row['org_norm']} (rows: {row['count']})\n")
            f.write(f"- assignee_ids: {row['assignee_ids']}\n")
            f.write(f"- org_names: {row['org_names']}\n\n")
