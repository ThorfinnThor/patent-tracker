from __future__ import annotations

import csv
import json
import os
import glob
from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class BuildConfig:
    sector_id: str
    store_dir: str              # data/store/<sector>/
    out_public_dir: str         # apps/web/public/data/<sector>/


def _safe_int(x: str) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0


def _load_store(store_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(store_dir, "pairs_*.csv")))
    if not files:
        raise FileNotFoundError(f"No partitions found in {store_dir}")
    parts = [pd.read_csv(f, dtype=str) for f in files]
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        raise RuntimeError(f"No data in {store_dir}")
    return df


def build_sector_artifacts(cfg: BuildConfig) -> None:
    df = _load_store(cfg.store_dir)
    df["assignee_type"] = df["assignee_type"].fillna("").astype(str)

    corp_types = {"2", "3"}
    corp = df[df["assignee_type"].isin(list(corp_types))].copy()
    if corp.empty:
        corp = df.copy()

    corp["cited_by"] = corp["patent_num_times_cited_by_us_patents"].fillna("0").map(_safe_int)

    def explode_cpcs(series: pd.Series) -> List[str]:
        out: List[str] = []
        for s in series.fillna(""):
            if not s:
                continue
            out.extend([x for x in str(s).split("|") if x])
        return out

    grouped = []
    for company_id, sub in corp.groupby("canonical_company_id", sort=False):
        patent_ids = set(sub["patent_id"].tolist())
        patent_count = len(patent_ids)

        total_citations = int(sub.drop_duplicates(subset=["patent_id"])["cited_by"].sum())
        cpcs = set(explode_cpcs(sub.drop_duplicates(subset=["patent_id"])["cpc_subclass_ids"]))
        breadth = len(cpcs)

        display_name = sub["display_name"].dropna().astype(str).iloc[0] if len(sub) else company_id

        grouped.append({
            "companyId": company_id,
            "displayName": display_name,
            "patentCount": patent_count,
            "totalCitations": total_citations,
            "citationsPerPatent": (total_citations / patent_count) if patent_count else 0.0,
            "cpcBreadth": breadth,
        })

    companies = pd.DataFrame(grouped).sort_values(
        ["patentCount", "totalCitations"], ascending=[False, False]
    ).head(200)

    out_sector_dir = cfg.out_public_dir
    patents_dir = os.path.join(out_sector_dir, "patents")
    os.makedirs(patents_dir, exist_ok=True)

    keep_cols = [
        "patent_id",
        "patent_date",
        "patent_title",
        "patent_num_times_cited_by_us_patents",
        "cpc_subclass_ids",
    ]

    for _, row in companies.iterrows():
        cid = row["companyId"]
        sub = corp[corp["canonical_company_id"] == cid].copy()
        sub = sub.drop_duplicates(subset=["patent_id"])[keep_cols]
        sub = sub.sort_values(["patent_date", "patent_id"], ascending=[False, False])

        out_csv = os.path.join(patents_dir, f"{cid}.csv")
        sub.to_csv(out_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    os.makedirs(out_sector_dir, exist_ok=True)
    out_companies_json = os.path.join(out_sector_dir, "companies.json")
    with open(out_companies_json, "w", encoding="utf-8") as f:
        json.dump(companies.to_dict(orient="records"), f, indent=2)

    out_companies_csv = os.path.join(out_sector_dir, "companies.csv")
    companies.to_csv(out_companies_csv, index=False, quoting=csv.QUOTE_MINIMAL)
