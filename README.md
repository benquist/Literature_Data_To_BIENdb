# Literature Data To BIENdb

This project implements a literature-to-BIEN ingestion workflow that handles a growing multi-paper portfolio of high-value plant occurrence and trait datasets. Each paper is parsed, normalized to Darwin Core, and staged for BIEN database ingestion.

## Pipeline Status (as of 2026-04-29)

- **~10+ papers processed:** jennings_2026, gosline_2023, novikov_2022, dayneko_2023, joyce_2020, aung_2025, and others
- **~14,325 georeferenced occurrence records** compiled across all papers
- Interactive report with per-paper breakdowns and a georeferenced leaflet map: `reports/literature_data_overview.html`
- All output follows Darwin Core schema with BIEN staging fields (GNRS-ready political units, `source_file`, `sourceProvenance`, DOI metadata)

### Migrated occurrence intake program (from DryadPlantTraits)

- Occurrence intake registry: **46 sources** (**10 compiled**, **33 pending_review**, **3 pending_manual_access**)
- Compiled manual occurrence records: **165,155 rows**, **144,389 georeferenced**
- Migrated scripts location: `scripts/occurrence_intake/`
- Migrated data location: `data/occurrences/` and `data/occurrence_source_intake.csv`

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

`scripts/run_pipeline.R` supports `--paper-id=...` and `--force=TRUE/FALSE`.
Individual step scripts support additional path overrides (for example `--interim-dir=...`, `--processed-dir=...`) where applicable.

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

## Coordinate And Elevation Mapping

The normalizer now preserves explicit location/elevation columns in every run, even when source data is missing values.

- Coordinate inputs accepted include `decimalLatitude`/`decimalLongitude` and common variants such as `lat`, `latitude`, `lon`, `long`, `lng`.
- Elevation inputs accepted include `verbatimElevation` and common variants such as `elev`, `elevation`, `elevation_m`, `alt`, `altitude`.
- If only a single elevation value is provided, it is propagated to `minimumElevationInMeters` and `maximumElevationInMeters`.
- Output tables always retain coordinate/elevation columns; rows without source values remain `NA` rather than dropping columns.

## Jennings 2026 Extraction Design (Occurrence + GNRS + Traits)

The Jennings parser now inspects all tabular supplementary sheets and extracts a richer occurrence payload aligned to BIEN `ViewFullOccurrence` style fields when present in source files.

Primary occurrence fields now extracted in `data/interim/jennings_2026_dwc_normalized.csv`:

- Core IDs: `occurrenceID` (source-supplied or deterministic generated fallback), `catalogNumber`, `recordNumber`
- Taxon + record context: `scientificName`, `scientificNameAuthorship`, `family`, `genus`, `basisOfRecord`, `occurrenceStatus`, `establishmentMeans`
- Locality and political units: `locality`, `verbatimLocality`, `localityNotes`, `country`, `countryCode`, `stateProvince`, `county`, `municipality`, `island`, `islandGroup`, `waterBody`
- Coordinates/georeference: `decimalLatitude`, `decimalLongitude`, `coordinateUncertaintyInMeters`, `geodeticDatum`, `georeferenceRemarks`
- Elevation: `verbatimElevation`, `minimumElevationInMeters`, `maximumElevationInMeters`, `elevation`
- Event time: `eventDate`, `eventYear`, `eventMonth`, `eventDay`
- Provenance: `source_file`, `source_sheet`, `original_row_number`, `sourceProvenance`, citation and DOI metadata

GNRS-ready political context is preserved in staging as:

- `country`, `state_province`, `county_parish`, `municipality`
- Raw counterparts for traceability: `country_raw`, `state_province_raw`, `county_raw`, `municipality_raw`, `political_units_raw`

This keeps direct GNRS input fields and original text side-by-side for auditing and re-processing.

## Habit/Growth Form Sidecar

If metadata includes explicit habit/growth-form values, the pipeline writes:

- `data/processed/jennings_2026_habit_traits.csv`

Schema includes one row per qualifying observation with `trait_name = growth_form`, raw/standardized trait values, observation linkage, and source provenance.

If no explicit habit values are found, the file is still written as a zero-row CSV with the same schema and an explicit log note so downstream steps remain idempotent and predictable.

## BIEN Service Alignment

This scaffold targets BIEN Data Loader style preprocessing and keeps compatibility with downstream TNRS/GNRS/GVS/NSR enrichment by producing explicit BIEN staging fields (`name_submitted`, `latitude`, `longitude`, geography, citation/provenance fields).

TNRS/GNRS/GVS/NSR calls are intentionally left as the next layer so this project can first establish robust literature ingestion and mapping.