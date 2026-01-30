from __future__ import annotations

import os
from typing import Dict, List, Set, Tuple

import pandas as pd

from pv_client import PVClient


def _derive_main_group(group_id: str) -> str:
    group_id = (group_id or "").strip().upper()
    if "/" not in group_id:
        return group_id
    left = group_id.split("/", 1)[0]
    return f"{left}/00"


def update_cpc_titles(client: PVClient, patents_csv_paths: List[str], out_pg_dir: str) -> None:
    """
    Build CPC dictionary tables (titles) for:

      - cpc_group (covers 'group' and 'main_group')
      - cpc_subclass
      - cpc_class

    Sources: the already-built postgres patent exports (biotech_patents.csv, tech_patents.csv).
    Output:
      data/state/postgres/cpc_group.csv
      data/state/postgres/cpc_subclass.csv
      data/state/postgres/cpc_class.csv
    """
    os.makedirs(out_pg_dir, exist_ok=True)

    group_ids: Set[str] = set()
    subclass_ids: Set[str] = set()
    class_ids: Set[str] = set()

    for p in patents_csv_paths:
        if not os.path.exists(p):
            continue
        df = pd.read_csv(p, dtype=str)
        for v in df.get("cpc_group_ids", pd.Series([], dtype=str)).fillna("").astype(str):
            for code in v.split("|"):
                code = code.strip().upper()
                if not code:
                    continue
                group_ids.add(code)
                group_ids.add(_derive_main_group(code))

        for v in df.get("cpc_subclass_ids", pd.Series([], dtype=str)).fillna("").astype(str):
            for code in v.split("|"):
                code = code.strip().upper()
                if not code:
                    continue
                subclass_ids.add(code)
                if len(code) >= 3:
                    class_ids.add(code[:3])

    def fetch_titles(endpoint: str, response_key_guess: str, id_field: str, title_field: str, ids: List[str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        want = [x for x in ids if x]
        if not want:
            return out

        batch_size = 200
        for i in range(0, len(want), batch_size):
            batch = want[i : i + batch_size]
            # PatentsView classification endpoints accept q like {idField: [ids...]} (as used in the other project)
            q = {id_field: batch}
            f = [id_field, title_field]
            s = [{id_field: "asc"}]
            data = client.request(endpoint=endpoint, q=q, f=f, s=s, o={"size": 1000})

            # Find list payload
            records = data.get(response_key_guess)
            if not isinstance(records, list):
                records = next((v for v in data.values() if isinstance(v, list)), [])

            for r in records:
                cid = str(r.get(id_field, "")).strip().upper()
                title = str(r.get(title_field, "")).strip()
                if cid and title:
                    out[cid] = title
        return out

    groups_sorted = sorted(group_ids)
    subs_sorted = sorted(subclass_ids)
    classes_sorted = sorted(class_ids)

    group_titles = fetch_titles("cpc_group", "cpc_groups", "cpc_group_id", "cpc_group_title", groups_sorted)
    subclass_titles = fetch_titles("cpc_subclass", "cpc_subclasses", "cpc_subclass_id", "cpc_subclass_title", subs_sorted)
    class_titles = fetch_titles("cpc_class", "cpc_classes", "cpc_class_id", "cpc_class_title", classes_sorted)

    pd.DataFrame(
        [{"cpc_group_id": k, "cpc_group_title": v} for k, v in sorted(group_titles.items())]
    ).to_csv(os.path.join(out_pg_dir, "cpc_group.csv"), index=False)

    pd.DataFrame(
        [{"cpc_subclass_id": k, "cpc_subclass_title": v} for k, v in sorted(subclass_titles.items())]
    ).to_csv(os.path.join(out_pg_dir, "cpc_subclass.csv"), index=False)

    pd.DataFrame(
        [{"cpc_class_id": k, "cpc_class_title": v} for k, v in sorted(class_titles.items())]
    ).to_csv(os.path.join(out_pg_dir, "cpc_class.csv"), index=False)
