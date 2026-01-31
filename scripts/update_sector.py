from __future__ import annotations

import json
import os
import glob
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Set, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta

from pv_client import PVClient
from normalize import load_assignee_map, map_assignee, normalize_name_for_suggestions


@dataclass(frozen=True)
class SectorConfig:
    sector_id: str
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
    return {"_or": [{"_begins": {"cpc_current.cpc_subclass_id": p}} for p in prefixes]}


def _load_partitioned_store(store_dir: str, prefix: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(store_dir, f"{prefix}_*.csv")))
    if not files:
        return pd.DataFrame()
    parts = [pd.read_csv(f, dtype=str) for f in files]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _write_partitioned_store(df: pd.DataFrame, store_dir: str, prefix: str) -> None:
    os.makedirs(store_dir, exist_ok=True)
    if df.empty:
        return

    df = df.copy()
    df["year"] = df["patent_date"].astype(str).str.slice(0, 4)

    for y, part in df.groupby("year"):
        out = os.path.join(store_dir, f"{prefix}_{y}.csv")
        part = part.drop(columns=["year"])
        part.to_csv(out, index=False)


def update_sector_pairs(
    client: PVClient,
    sector: SectorConfig,
    assignee_map_path: str,
    last_run_path: str,
    out_store_dir: str,
) -> None:
    os.makedirs(out_store_dir, exist_ok=True)

    existing_pairs = _load_partitioned_store(out_store_dir, "pairs")
    existing_inventors = _load_partitioned_store(out_store_dir, "inventors")

    last_run = load_last_run(last_run_path)

    # ✅ Allow overrides for fast mode
    today = os.environ.get("WINDOW_END_ISO", "").strip() or _today_iso()
    start_date = os.environ.get("WINDOW_START_ISO", "").strip() or _five_years_ago_iso()

    fields = [
        "patent_id",
        "patent_title",
        "patent_date",
        "patent_num_times_cited_by_us_patents",
        "cpc_current.cpc_subclass_id",
        "cpc_current.cpc_group_id",
        "assignees.assignee_id",
        "assignees.assignee_organization",
        "assignees.assignee_type",
        "inventors.inventor_id",
        "inventors.inventor_name_first",
        "inventors.inventor_name_last",
    ]
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

    new_pair_rows: List[Dict[str, str]] = []
    new_inv_rows: List[Dict[str, str]] = []

    seen_new_pairs: Set[Tuple[str, str, str]] = set()
    seen_new_invs: Set[Tuple[str, str, str]] = set()

    for page in client.paginate(endpoint="patent", q=q, f=fields, s=sort, size=1000):
        patents = page.get("patents", []) or []
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

            cpcs = p.get("cpc_current", []) or []
            cpc_subs = sorted({(c.get("cpc_subclass_id") or "").strip() for c in cpcs if c.get("cpc_subclass_id")})
            cpc_groups = sorted({(c.get("cpc_group_id") or "").strip().upper() for c in cpcs if c.get("cpc_group_id")})

            cpc_subs_str = "|".join([x for x in cpc_subs if x])
            cpc_groups_str = "|".join([x for x in cpc_groups if x])

            inventors = p.get("inventors", []) or []
            inv_norm: List[Tuple[str, str, str, str]] = []
            for inv in inventors:
                inv_id = (inv.get("inventor_id") or "").strip()
                if not inv_id:
                    continue
                first = (inv.get("inventor_name_first") or "").strip()
                last = (inv.get("inventor_name_last") or "").strip()
                full = (f"{first} {last}").strip()
                inv_norm.append((inv_id, first, last, full))

            for a in (p.get("assignees", []) or []):
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

                new_pair_rows.append(
                    {
                        "sector_id": sector.sector_id,
                        "patent_id": patent_id,
                        "patent_date": patent_date,
                        "patent_title": patent_title,
                        "patent_num_times_cited_by_us_patents": cited_by_str,
                        "cpc_subclass_ids": cpc_subs_str,
                        "cpc_group_ids": cpc_groups_str,
                        "assignee_id": assignee_id,
                        "assignee_type": assignee_type,
                        "assignee_organization": assignee_org,
                        "canonical_company_id": canonical_company_id,
                        "display_name": display_name,
                    }
                )

                for inv_id, first, last, full in inv_norm:
                    ikey = (patent_id, canonical_company_id, inv_id)
                    if ikey in seen_new_invs:
                        continue
                    seen_new_invs.add(ikey)

                    new_inv_rows.append(
                        {
                            "sector_id": sector.sector_id,
                            "canonical_company_id": canonical_company_id,
                            "patent_id": patent_id,
                            "patent_date": patent_date,
                            "inventor_id": inv_id,
                            "inventor_name_first": first,
                            "inventor_name_last": last,
                            "inventor_name": full,
                        }
                    )

    if new_pair_rows:
        new_pairs_df = pd.DataFrame(new_pair_rows)
        combined = pd.concat([existing_pairs, new_pairs_df], ignore_index=True) if not existing_pairs.empty else new_pairs_df
        combined.drop_duplicates(subset=["sector_id", "patent_id", "canonical_company_id", "assignee_id"], inplace=True)
        _write_partitioned_store(combined, out_store_dir, "pairs")

    if new_inv_rows:
        new_inv_df = pd.DataFrame(new_inv_rows)
        combined_inv = pd.concat([existing_inventors, new_inv_df], ignore_index=True) if not existing_inventors.empty else new_inv_df
        combined_inv.drop_duplicates(subset=["sector_id", "patent_id", "canonical_company_id", "inventor_id"], inplace=True)
        _write_partitioned_store(combined_inv, out_store_dir, "inventors")

    last_run[sector.sector_id] = {"refreshed_at": today, "window_start": start_date, "window_end": today}
    save_last_run(last_run_path, last_run)


def write_normalization_suggestions(store_dir: str, out_md_path: str) -> None:
    df = _load_partitioned_store(store_dir, "pairs")
    if df.empty:
        return

    df = df.copy()
    df["assignee_organization_norm"] = df["assignee_organization"].fillna("").map(normalize_name_for_suggestions)

    top = (
        df.groupby(["canonical_company_id", "display_name", "assignee_organization_norm"], dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
        .head(300)
    )

    os.makedirs(os.path.dirname(out_md_path), exist_ok=True)
    with open(out_md_path, "w", encoding="utf-8") as f:
        f.write("# Normalization suggestions\n\n")
        f.write("Most frequent assignee organization strings per canonical company (top 300 rows):\n\n")
        f.write("| canonical_company_id | display_name | assignee_org_norm | n |\n")
        f.write("|---|---|---|---|\n")
        for _, r in top.iterrows():
            f.write(
                f"| {r['canonical_company_id']} | {r['display_name']} | {r['assignee_organization_norm']} | {int(r['n'])} |\n"
            )
