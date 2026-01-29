from __future__ import annotations

import csv
import glob
import json
import os
import sqlite3
from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class BuildConfig:
    sector_id: str
    store_dir: str              # data/store/<sector>/
    out_public_dir: str         # apps/web/public/data/<sector>/
    out_db_path: str            # apps/web/data/db/<sector>.sqlite


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

    # Write companies.json for fast sector page load
    os.makedirs(cfg.out_public_dir, exist_ok=True)
    out_companies_json = os.path.join(cfg.out_public_dir, "companies.json")
    with open(out_companies_json, "w", encoding="utf-8") as f:
        json.dump(companies.to_dict(orient="records"), f, indent=2)

    out_companies_csv = os.path.join(cfg.out_public_dir, "companies.csv")
    companies.to_csv(out_companies_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    # Build SQLite DB (queryable store)
    os.makedirs(os.path.dirname(cfg.out_db_path), exist_ok=True)
    if os.path.exists(cfg.out_db_path):
        os.remove(cfg.out_db_path)

    conn = sqlite3.connect(cfg.out_db_path)
    cur = conn.cursor()

    cur.execute("""
      CREATE TABLE patents (
        company_id TEXT NOT NULL,
        patent_id TEXT NOT NULL,
        patent_date TEXT NOT NULL,
        patent_year INTEGER NOT NULL,
        patent_title TEXT NOT NULL,
        cited_by INTEGER NOT NULL,
        cpc_subclass_ids TEXT NOT NULL,
        PRIMARY KEY(company_id, patent_id)
      )
    """)

    # Insert only patents for top 200 companies (keeps DB bounded)
    top_company_ids = set(companies["companyId"].astype(str).tolist())
    corp = corp[corp["canonical_company_id"].astype(str).isin(top_company_ids)].copy()

    corp["patent_year"] = corp["patent_date"].astype(str).str.slice(0, 4).fillna("0")
    corp["patent_year"] = corp["patent_year"].apply(lambda s: int(s) if str(s).isdigit() else 0)

    rows = []
    for _, r in corp.iterrows():
        rows.append((
            str(r["canonical_company_id"]),
            str(r["patent_id"]),
            str(r["patent_date"]),
            int(r["patent_year"]),
            str(r.get("patent_title") or ""),
            int(r.get("cited_by") or 0),
            str(r.get("cpc_subclass_ids") or ""),
        ))

    cur.executemany("""
      INSERT OR REPLACE INTO patents
      (company_id, patent_id, patent_date, patent_year, patent_title, cited_by, cpc_subclass_ids)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)

    # Indices for fast browsing
    cur.execute("CREATE INDEX idx_patents_company_date ON patents(company_id, patent_date DESC)")
    cur.execute("CREATE INDEX idx_patents_company_cited ON patents(company_id, cited_by DESC, patent_date DESC)")
    cur.execute("CREATE INDEX idx_patents_company_year ON patents(company_id, patent_year DESC)")

    conn.commit()
    conn.close()
