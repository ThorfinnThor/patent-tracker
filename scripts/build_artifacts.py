# ... imports unchanged ...

from build_artifacts import BuildConfig, build_sector_artifacts
from update_sector import SectorConfig, update_sector_pairs, write_normalization_suggestions

# inside main():

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
        out_path=os.path.join(root, "data", "state", f"normalization_suggestions_{sector.sector_id}.md"),
    )

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
