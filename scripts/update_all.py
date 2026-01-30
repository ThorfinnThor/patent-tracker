from __future__ import annotations

import os

from pv_client import PVClient
from update_sector import (
    SectorConfig,
    update_sector_pairs,
    write_normalization_suggestions,
)
from build_artifacts import BuildConfig, build_sector_artifacts
from update_cpc_titles import update_cpc_titles


def main() -> None:
    api_key = os.environ.get("PATENTSVIEW_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing PATENTSVIEW_API_KEY env var")

    client = PVClient(api_key=api_key)

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    assignee_map = os.path.join(root, "data", "normalization", "assignee_map.yml")
    last_run = os.path.join(root, "data", "state", "last_run.json")
    pg_dir = os.path.join(root, "data", "state", "postgres")

    biotech = SectorConfig(
        sector_id="biotech",
        cpc_subclass_prefixes=["A61K", "A61P", "C07K", "C12N", "C12P", "C12Q", "C12Y", "G01N"],
    )
    tech = SectorConfig(
        sector_id="tech",
        cpc_subclass_prefixes=["G06F", "G06Q", "G06T", "G06N", "H04L", "H04W", "H04N", "H01L"],
    )

    for sector in [biotech, tech]:
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

    # Build CPC dictionaries for the UI (titles)
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
