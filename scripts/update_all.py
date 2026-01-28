from __future__ import annotations

import os

from pv_client import PVClient
from update_sector import (
    SectorConfig,
    update_sector_pairs,
    write_normalization_suggestions,
)
from build_artifacts import BuildConfig, build_sector_artifacts


def main() -> None:
    api_key = os.environ.get("PATENTSVIEW_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing PATENTSVIEW_API_KEY env var")

    client = PVClient(api_key=api_key)

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    assignee_map = os.path.join(root, "data", "normalization", "assignee_map.yml")
    last_run = os.path.join(root, "data", "state", "last_run.json")

    # Sector definitions by CPC subclass prefixes (editable)
    biotech = SectorConfig(
        sector_id="biotech",
        cpc_subclass_prefixes=[
            "A61K", "A61P", "C07K", "C12N", "C12P", "C12Q", "C12Y", "G01N"
        ],
    )
    tech = SectorConfig(
        sector_id="tech",
        cpc_subclass_prefixes=[
            "G06F", "G06Q", "G06T", "G06N", "H04L", "H04W", "H04N", "H01L"
        ],
    )

    # Update partitioned stores (data/store/<sector>/pairs_<YYYY>.csv)
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
            out_path=os.path.join(
                root,
                "data",
                "state",
                f"normalization_suggestions_{sector.sector_id}.md",
            ),
        )

    # Build web artifacts into apps/web/public/data/<sector>/
    for sector_id in ["biotech", "tech"]:
        store_dir = os.path.join(root, "data", "store", sector_id)
        out_public = os.path.join(root, "apps", "web", "public", "data", sector_id)

        build_sector_artifacts(
            BuildConfig(
                sector_id=sector_id,
                store_dir=store_dir,
                out_public_dir=out_public,
            )
        )


if __name__ == "__main__":
    main()
