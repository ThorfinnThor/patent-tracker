from __future__ import annotations

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


def _load_store(store_dir: str, prefix: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(store_dir, f"{prefix}_*.csv")))
    if not files:
        return pd.DataFrame()
    parts = [pd.read_csv(f, dtype=str) for f in files]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def build_sector_artifacts(cfg: BuildConfig) -> None:
    os.makedirs(cfg.out_public_dir, exist_ok=True)
    os.makedirs(cfg.out_pg_dir, exist_ok=True)

    pairs = _load_store(cfg.store_dir, "pairs")
    if pairs.empty:
        raise RuntimeError(f"No pairs store found under {cfg.store_dir}")

    # Keep only corporations/companies as "tracked companies"
    # (Assignee type codes vary; "2" is commonly "US Company/Corporation".)
    pairs["assignee_type"] = pairs["assignee_type"].fillna("").astype(str)
    corp = pairs[pairs["assignee_type"] == "2"].copy()

    # Normalize numeric + date fields
    corp["cited_by"] = corp["patent_num_times_cited_by_us_patents"].fillna("0").map(_safe_int)
    corp["patent_date"] = corp["patent_date"].fillna("").astype(str)
    corp["patent_year"] = corp["patent_date"].str.slice(0, 4).map(lambda x: _safe_int(x) if x else 0)

    # Company ranking criterion: patent count (stable, computed from one source)
    company_stats = (
        corp.groupby(["canonical_company_id", "display_name"], dropna=False)
        .agg(
            patentCount=("patent_id", "nunique"),
            totalCitations=("cited_by", "sum"),
        )
        .reset_index()
    )
    company_stats["citationsPerPatent"] = company_stats.apply(
        lambda r: (float(r["totalCitations"]) / float(r["patentCount"])) if r["patentCount"] else 0.0, axis=1
    )

    # CPC breadth: unique CPC subclasses across 5y
    def _cpc_breadth(series: pd.Series) -> int:
        s: set[str] = set()
        for v in series.fillna("").astype(str):
            for code in v.split("|"):
                code = code.strip()
                if code:
                    s.add(code)
        return len(s)

    breadth = (
        corp.groupby(["canonical_company_id"], dropna=False)["cpc_subclass_ids"]
        .apply(_cpc_breadth)
        .reset_index(name="cpcBreadth")
    )
    company_stats = company_stats.merge(breadth, on="canonical_company_id", how="left")
    company_stats["cpcBreadth"] = company_stats["cpcBreadth"].fillna(0).astype(int)

    # Top 200 by patentCount
    top = company_stats.sort_values(["patentCount", "totalCitations"], ascending=[False, False]).head(200).copy()

    # Write companies.json for the UI
    companies_out = top.rename(
        columns={
            "canonical_company_id": "companyId",
            "display_name": "displayName",
        }
    )[
        ["companyId", "displayName", "patentCount", "totalCitations", "citationsPerPatent", "cpcBreadth"]
    ].to_dict(orient="records")

    with open(os.path.join(cfg.out_public_dir, "companies.json"), "w", encoding="utf-8") as f:
        json.dump(companies_out, f, indent=2)

    # Postgres exports (patents + companies + inventors)
    top_ids = set(top["canonical_company_id"].astype(str))

    # Patents table (company-patent rows)
    patents_df = corp[corp["canonical_company_id"].astype(str).isin(top_ids)].copy()
    patents_df["sector"] = cfg.sector_id
    patents_df["company_id"] = patents_df["canonical_company_id"].astype(str)
    patents_df["patent_id"] = patents_df["patent_id"].astype(str)

    # Deduplicate to unique company/patent
    patents_df = patents_df.sort_values(["patent_date", "patent_id"]).drop_duplicates(
        subset=["company_id", "patent_id"], keep="last"
    )

    pg_patents = patents_df[
        [
            "sector",
            "company_id",
            "patent_id",
            "patent_date",
            "patent_year",
            "patent_title",
            "cited_by",
            "cpc_subclass_ids",
            "cpc_group_ids",
        ]
    ].copy()

    pg_patents.to_csv(os.path.join(cfg.out_pg_dir, f"{cfg.sector_id}_patents.csv"), index=False)

    # Companies table export (same as companies.json but with "sector" column)
    pg_companies = top.rename(
        columns={
            "canonical_company_id": "companyId",
            "display_name": "displayName",
        }
    )[
        ["companyId", "displayName", "patentCount", "totalCitations", "citationsPerPatent", "cpcBreadth"]
    ].copy()
    pg_companies.insert(0, "sector", cfg.sector_id)
    pg_companies.to_csv(os.path.join(cfg.out_pg_dir, f"{cfg.sector_id}_companies.csv"), index=False)

    # Inventors export for top companies
    inventors = _load_store(cfg.store_dir, "inventors")
    if not inventors.empty:
        inventors = inventors[inventors["canonical_company_id"].astype(str).isin(top_ids)].copy()
        inventors["sector"] = cfg.sector_id
        inventors["company_id"] = inventors["canonical_company_id"].astype(str)
        inventors = inventors.drop_duplicates(subset=["sector", "company_id", "patent_id", "inventor_id"])

        pg_inventors = inventors[
            [
                "sector",
                "company_id",
                "patent_id",
                "inventor_id",
                "inventor_name_first",
                "inventor_name_last",
                "inventor_name",
                "patent_date",
            ]
        ].copy()

        pg_inventors.to_csv(os.path.join(cfg.out_pg_dir, f"{cfg.sector_id}_inventors.csv"), index=False)
    else:
        # Write an empty file with headers so workflow COPY doesn't fail
        empty = pd.DataFrame(
            columns=[
                "sector",
                "company_id",
                "patent_id",
                "inventor_id",
                "inventor_name_first",
                "inventor_name_last",
                "inventor_name",
                "patent_date",
            ]
        )
        empty.to_csv(os.path.join(cfg.out_pg_dir, f"{cfg.sector_id}_inventors.csv"), index=False)
