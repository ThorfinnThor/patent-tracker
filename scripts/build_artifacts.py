from __future__ import annotations

import csv
import glob
import json
import os
from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class BuildConfig:
    sector_id: str
    store_dir: str              # data/store/<sector>/
    out_public_dir: str         # apps/web/public/data/<sector>/
    out_pg_dir: str             # data/state/postgres/


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

    # Deduplicate per (company, patent)
    corp = corp.drop_duplicates(subset=["canonical_company_id", "patent_id"], keep="last")

    # Compute company metrics
    def explode_cpcs(series: pd.Series) -> List[str]:
        out: List[str] = []
        for s in series.fillna(""):
            if not s:
                continue
            out.extend([x for x in str(s).split("|") if x])
        return out

    grouped = []
    for company_id, sub in corp.groupby("canonical_company_id", sort=False):
        patent_count = int(sub["patent_id"].nunique())
        total_citations = int(sub["cited_by"].sum())
        cpcs = set(explode_cpcs(sub["cpc_subclass_ids"]))
        breadth = len(cpcs)
        display_name = sub["display_name"].dropna().astype(str).iloc[0] if len(sub) else company_id

        grouped.append({
            "sector": cfg.sector_id,
            "companyId": company_id,
            "displayName": display_name,
            "patentCount": patent_count,
            "totalCitations": total_citations,
            "citationsPerPatent": (total_citations / patent_count) if patent_count else 0.0,
            "cpcBreadth": breadth,
        })

    companies = (
        pd.DataFrame(grouped)
        .sort_values(["patentCount", "totalCitations"], ascending=[False, False])
        .head(200)
    )

    # Write companies.json for fast sector pages
    os.makedirs(cfg.out_public_dir, exist_ok=True)
    out_companies_json = os.path.join(cfg.out_public_dir, "companies.json")
    with open(out_companies_json, "w", encoding="utf-8") as f:
        json.dump(
            companies.drop(columns=["sector"]).to_dict(orient="records"),
            f,
            indent=2,
        )

    out_companies_csv = os.path.join(cfg.out_public_dir, "companies.csv")
    companies.drop(columns=["sector"]).to_csv(out_companies_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    # Export Postgres load CSVs (top 200 only)
    os.makedirs(cfg.out_pg_dir, exist_ok=True)

    top_ids = set(companies["companyId"].astype(str).tolist())
    sub = corp[corp["canonical_company_id"].astype(str).isin(top_ids)].copy()

    sub["patent_year"] = sub["patent_date"].astype(str).str.slice(0, 4)
    sub["patent_year"] = sub["patent_year"].apply(lambda s: int(s) if str(s).isdigit() else 0)

    # patents CSV
    patents_out = os.path.join(cfg.out_pg_dir, f"{cfg.sector_id}_patents.csv")
    patents_df = pd.DataFrame({
        "sector": cfg.sector_id,
        "company_id": sub["canonical_company_id"].astype(str),
        "patent_id": sub["patent_id"].astype(str),
        "patent_date": sub["patent_date"].astype(str),
        "patent_year": sub["patent_year"].astype(int),
        "patent_title": sub["patent_title"].fillna("").astype(str),
        "cited_by": sub["cited_by"].astype(int),
        "cpc_subclass_ids": sub["cpc_subclass_ids"].fillna("").astype(str),
    }).drop_duplicates(subset=["sector", "company_id", "patent_id"], keep="last")

    patents_df.to_csv(patents_out, index=False, quoting=csv.QUOTE_MINIMAL)

    # companies CSV (for DB)
    companies_out = os.path.join(cfg.out_pg_dir, f"{cfg.sector_id}_companies.csv")
    companies.to_csv(companies_out, index=False, quoting=csv.QUOTE_MINIMAL)
