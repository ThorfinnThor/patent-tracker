from __future__ import annotations

import os
from datetime import date
from dateutil.relativedelta import relativedelta

from pv_client import PVClient
from update_sector import (
    SectorConfig,
    update_sector_pairs,
    write_normalization_suggestions,
)
from build_artifacts import BuildConfig, build_sector_artifacts
from update_cpc_titles import update_cpc_titles


def _today_iso() -> str:
    return date.today().isoformat()


def _days_ago_iso(days: int) -> str:
    return (date.today() - relativedelta(days=days)).isoformat()


def main() -> None:
    api_key = os.environ.get("PATENTSVIEW_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing PATENTSVIEW_API_KEY env var")

    # FAST_MODE=1 => much faster runs (shorter window, fewer companies, optional skip CPC titles)
    fast_mode = os.environ.get("FAST_MODE", "0").strip() == "1"
    fast_days = int(os.environ.get("FAST_DAYS", "90"))  # default 90 days in fast mode
    only_sector = os.environ.get("ONLY_SECTOR", "").strip()  # optional: "tech" or "biotech"

    client = PVClient(api_key=api_key)

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    assignee_map = os.path.join(root, "data", "normalization", "assignee_map.yml")
    last_run = os.path.join(root, "data", "state", "last_run.json")
    pg_dir = os.path.join(root, "data", "state", "postgres")

    # Sector definitions
    biotech = SectorConfig(
        sector_id="biotech",
        cpc_subclass_prefixes=["A61K", "A61P", "C07K", "C12N", "C12P", "C12Q", "C12Y", "G01N"],
    )
    tech = SectorConfig(
        sector_id="tech",
        cpc_subclass_prefixes=["G06F", "G06Q", "G06T", "G06N", "H04L", "H04W", "H04N", "H01L"],
    )

    sectors = [biotech, tech]
    if only_sector in ["biotech", "tech"]:
        sectors = [s for s in sectors if s.sector_id == only_sector]

    # In fast mode we overwrite the window to a smaller one.
    # update_sector_pairs currently uses "last 5 years" internally; we pass override env vars
    # to let it shorten the window without changing function signature.
    if fast_mode:
        os.environ["WINDOW_START_ISO"] = _days_ago_iso(fast_days)
        os.environ["WINDOW_END_ISO"] = _today_iso()
        os.environ["TOP_N_COMPANIES"] = os.environ.get("TOP_N_COMPANIES", "50")  # default 50 in fast mode
    else:
        os.environ.pop("WINDOW_START_ISO", None)
        os.environ.pop("WINDOW_END_ISO", None)
        os.environ.pop("TOP_N_COMPANIES", None)

    for sector in sectors:
        store_dir = os.path.join(root, "data", "store", sector.sector_id)

        update_sector_pairs(
            client=client,
            sector=sector,
            assignee_map_path=assignee_map,
            last_run_path=last_run,
            out_store_dir=store_dir,
        )

        write_normalization_suggestions(
            store_dir=store_dir,
            out_md_path=os.path.join(root, "data", "state", f"normalization_suggestions_{sector.sector_id}.md"),
        )

        build_sector_artifacts(
            BuildConfig(
                sector_id=sector.sector_id,
                store_dir=store_dir,
                out_public_dir=os.path.join(root, "apps", "web", "public", "data", sector.sector_id),
                out_pg_dir=pg_dir,
            )
        )

    # CPC dictionaries: skip or cap in fast mode if desired
    skip_cpc_titles = os.environ.get("SKIP_CPC_TITLES", "0").strip() == "1"
    if fast_mode and skip_cpc_titles:
        return

    update_cpc_titles(
        client=client,
        patents_csv_paths=[
            os.path.join(pg_dir, "biotech_patents.csv"),
            os.path.join(pg_dir, "tech_patents.csv"),
        ],
        out_pg_dir=pg_dir,
    )


if __name__ == "__main__":
    main()
