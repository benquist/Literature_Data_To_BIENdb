# Literature Data To BIENdb

This project bootstraps a literature-to-BIEN ingestion workflow focused on individual papers with high-value observation data.

## Goal

For each paper:

1. Resolve DOI and fetch article metadata.
2. Discover machine-readable data links (supplementary files, repository assets, API links).
3. Download accessible source files to `data/raw`.
4. Parse and normalize source data into a Darwin Core-like table.
5. Map normalized records into a BIEN staging-ready CSV.
6. Keep run metadata and blockers in `logs/` for reproducibility.

## First Paper Bootstrap

Paper configured now:

- Jennings LVS, et al. (2026) PhytoKeys 273:21-36. doi:10.3897/phytokeys.273.184780
- Landing page: https://phytokeys.pensoft.net/article/184780/

Paper configuration is stored in `config/papers.csv`.
Paper-specific field mapping is in `mappings/jennings_2026_column_mapping.csv`.

## Folder Layout

- `config/` paper definitions
- `mappings/` paper-specific source-to-DWC/BIEN mappings
- `scripts/` executable workflow scripts
- `data/raw/` downloaded source files
- `data/interim/` discovered metadata and normalized DWC-like outputs
- `data/processed/` BIEN staging outputs
- `logs/` run logs and blocker diagnostics

## Scripts

- `scripts/01_discover_paper_assets.R`: DOI metadata + supplementary/data-link discovery.
- `scripts/02_download_sources.R`: idempotent source file downloads.
- `scripts/03_normalize_to_dwc.R`: parse tabular files and normalize to DWC-like fields.
- `scripts/04_build_bien_staging.R`: map normalized records into BIEN staging output.
- `scripts/run_pipeline.R`: top-level orchestrator for end-to-end run.

All scripts support `--paper-id=...` and `--output-dir=...` style CLI args.

## Run

From workspace root:

```bash
Rscript Literature_Data_To_BIENdb/scripts/run_pipeline.R --paper-id=jennings_2026
```

Optional:

```bash
Rscript Literature_Data_To_BIENdb/scripts/run_pipeline.R --paper-id=jennings_2026 --force=TRUE
```

`--force=TRUE` re-downloads files and rewrites outputs.

## BIEN Service Alignment

This scaffold targets BIEN Data Loader style preprocessing and keeps compatibility with downstream TNRS/GNRS/GVS/NSR enrichment by producing explicit BIEN staging fields (`name_submitted`, `latitude`, `longitude`, geography, citation/provenance fields).

TNRS/GNRS/GVS/NSR calls are intentionally left as the next layer so this project can first establish robust literature ingestion and mapping.