# Literature Data To BIENdb

A reproducible literature-to-BIEN ingestion workflow that converts a growing portfolio of published plant occurrence and checklist datasets into Darwin Core records and BIEN-staging-ready CSVs. Each paper is configured once, then automatically discovered, downloaded, normalized, and staged for downstream TNRS / GNRS / GVS / NSR enrichment via the [BIEN Data Loader](https://github.com/benquist/BIEN_Data_Loader) pipeline.

This is **not a single-paper project** — Jennings 2026 was the bootstrap case, but the pipeline is paper-agnostic and currently handles Pensoft (PhytoKeys, Biodiversity Data Journal), Springer/Nature Scientific Data, MDPI, and GBIF-hosted Darwin Core archives.

---

## Pipeline Status (2026-04-29)

### Literature portfolio
- **11 papers configured** in `config/papers.csv`
- **6 papers fully staged** to BIEN format (~176,150 occurrence/checklist rows)
- **5 papers pending** discovery-layer fixes (HTML fallback / GBIF UUID auto-fetch)

### Migrated occurrence-intake program (from DryadPlantTraits)
- **46 sources** in registry (`data/occurrence_source_intake.csv`)
  - 10 compiled · 33 pending_review · 3 pending_manual_access
- **165,155** compiled manual occurrence records (**144,389 georeferenced**)
- Scripts: `scripts/occurrence_intake/`
- Data: `data/occurrences/`

### Combined output
- ~14,325 georeferenced occurrence records mapped in `reports/literature_data_overview.html`
- All outputs follow Darwin Core + BIEN staging schema (GNRS-ready political units, `source_file`, `sourceProvenance`, DOI metadata)

---

## Data Sources Included

### A. Staged literature papers (BIEN-ready)

| paper_id | Region | Type | Publisher | Rows staged | DOI |
|---|---|---|---|---:|---|
| `joyce_2020` | Sunda-Sahul Convergence Zone (SE Asia) | Checklist | BDJ (Pensoft) | 146,116 | 10.3897/bdj.8.e51094 |
| `aung_2025` | Myanmar | National checklist | PhytoKeys | 14,020 | 10.3897/phytokeys.261.154986 |
| `novikov_2022` | Ukrainian Carpathians (endemic flora) | Occurrences (GBIF DwC-A) | BDJ | 6,935 | 10.3897/bdj.10.e95910 |
| `gosline_2023` | Republic of Guinea | Vouchered checklist (GBIF DwC-A) | Scientific Data | 4,028 | 10.1038/s41597-023-02236-6 |
| `dayneko_2023` | Lower Dnipro ancient settlements (Ukraine) | Occurrences (GBIF DwC-A) | BDJ | 3,210 | 10.3897/bdj.11.e99041 |
| `jennings_2026` | Lesser Sunda Islands (endemics) | Vouchered checklist | PhytoKeys | 1,841 | 10.3897/phytokeys.273.184780 |
| **Subtotal** | | | | **176,150** | |

### B. Configured but pending pipeline completion

| paper_id | Region | Blocker |
|---|---|---|
| `wasowicz_2020` | Surtsey Island, Iceland (1965-1990) | BDJ DwC-A supplementary not auto-discovered (XML fallback needed) |
| `moysiyenko_2023` | Kurgans, Southern Ukraine | Pensoft XML returns 0 assets — HTML scrape fallback needed |
| `tack_2022` (ECAT) | Central Africa endemic trees | PhytoKeys supplementary XLSX, no GBIF DwC-A |
| `salim_2020` | Setiu Wetlands, Terengganu, Malaysia | PhytoKeys XML asset discovery |
| `sun_2024` | Indonesia national checklist | MDPI supplementary, format TBD |

### C. Migrated occurrence-intake registry (formerly DryadPlantTraits)

A 46-source registry covering Dryad / Zenodo / GBIF / Pensoft direct-download pipelines. See `data/occurrence_source_intake.csv` for the full inventory. Notable already-compiled sources include the Kyrgyzstan Dryad checklist, SIVFLORA (Zenodo), PacIFlora (Dryad), Walker Russian Arctic, and the High-Andes Arroyo dataset.

---

## Workflow

For each paper, the pipeline runs six idempotent steps:

1. **Resolve DOI** and fetch article metadata (`01_discover_paper_assets.R`).
2. **Discover** machine-readable assets — supplementary XLSX/CSV, repository links, GBIF dataset UUIDs, GBIF DwC-A endpoints.
3. **Download** to `data/raw/<paper_id>/` (`02_download_sources.R`, idempotent).
4. **Normalize** tabular content to Darwin Core (`03_normalize_to_dwc.R`) → `data/interim/<paper>_dwc_normalized.csv`.
5. **Stage for BIEN** (`04_build_bien_staging.R`) → `data/processed/<paper>_bien_staging.csv` with BIEN field names alongside DwC counterparts.
6. **Log** run metadata + blockers in `logs/` for reproducibility.

Top-level orchestrator: `scripts/run_pipeline.R`.

```bash
# Run a single paper end-to-end
Rscript Literature_Data_To_BIENdb/scripts/run_pipeline.R --paper-id=novikov_2022

# Force re-download + rewrite outputs
Rscript Literature_Data_To_BIENdb/scripts/run_pipeline.R --paper-id=novikov_2022 --force=TRUE
```

Individual step scripts accept path overrides (e.g. `--interim-dir=...`, `--processed-dir=...`).

---

## Output Schema

### Darwin Core normalized (`data/interim/<paper>_dwc_normalized.csv`)

- **Core IDs:** `occurrenceID`, `catalogNumber`, `recordNumber`
- **Taxon:** `scientificName`, `scientificNameAuthorship`, `family`, `genus`, `basisOfRecord`, `occurrenceStatus`, `establishmentMeans`
- **Locality:** `locality`, `verbatimLocality`, `localityNotes`, `country`, `countryCode`, `stateProvince`, `county`, `municipality`, `island`, `islandGroup`, `waterBody`
- **Coordinates:** `decimalLatitude`, `decimalLongitude`, `coordinateUncertaintyInMeters`, `geodeticDatum`, `georeferenceRemarks`
- **Elevation:** `verbatimElevation`, `minimumElevationInMeters`, `maximumElevationInMeters`, `elevation`
- **Event time:** `eventDate`, `eventYear`, `eventMonth`, `eventDay`
- **Provenance:** `source_file`, `source_sheet`, `original_row_number`, `sourceProvenance`, citation, DOI

### BIEN staging (`data/processed/<paper>_bien_staging.csv`)

GNRS-ready political units alongside raw text counterparts:

- `country`, `state_province`, `county_parish`, `municipality`
- `country_raw`, `state_province_raw`, `county_raw`, `municipality_raw`, `political_units_raw`
- `name_submitted`, `latitude`, `longitude` (BIEN field names mirroring DwC)
- Citation/provenance: `source_file`, `sourceProvenance`, DOI

This format aligns with the [BIEN Data Loader](https://github.com/benquist/BIEN_Data_Loader) Shiny app's expected upload schema, so files can be loaded directly with no Step 2 column remapping.

### Habit / growth-form sidecar

When metadata includes explicit habit values, the pipeline writes `data/processed/<paper>_habit_traits.csv` (one row per qualifying observation, `trait_name = growth_form`, raw + standardized values, observation linkage, provenance). When no values are found the file is still written empty with the same schema for downstream idempotency.

### Per-paper summary

Each run also writes `data/processed/<paper>_staging_summary.csv` with row counts, georeferenced counts, and key field coverage.

---

## Coordinate And Elevation Mapping

The normalizer preserves explicit location/elevation columns in every run, even when source data is missing values.

- Coordinate input synonyms: `decimalLatitude`/`decimalLongitude`, `lat`, `latitude`, `lon`, `long`, `lng`.
- Elevation input synonyms: `verbatimElevation`, `elev`, `elevation`, `elevation_m`, `alt`, `altitude`.
- Single-value elevation propagates to both `minimumElevationInMeters` and `maximumElevationInMeters`.
- Output retains coordinate/elevation columns; missing rows are `NA` (columns never dropped).

---

## BIEN Service Alignment

This project produces BIEN-staging-ready CSVs that hand off cleanly to:

- **TNRS** taxonomic resolution → populates `scrubbed_*` fields
- **GNRS** geographic name resolution → standardizes `country`, `state_province`, `county`
- **GVS** geographic validation → adds `is_centroid`, coordinate-quality flags
- **NSR** native-status resolution → populates `native_status`, `is_introduced`, `is_cultivated_observation`

These enrichment steps are intentionally deferred to the BIEN Data Loader app rather than embedded here, so this project remains focused on robust ingestion, parsing, and DwC mapping.

---

## Adding A New Paper

1. Append a row to `config/papers.csv` with `paper_id`, `doi`, `citation`, `landing_url`, `xml_url` (if Pensoft), `expected_file_ids` (Pensoft asset IDs or GBIF dataset UUID), `notes`.
2. (Optional) Add a paper-specific column mapping at `mappings/<paper_id>_column_mapping.csv` if source columns need explicit aliasing.
3. Run `Rscript scripts/run_pipeline.R --paper-id=<paper_id>`.
4. Inspect `logs/<paper>.log`, `data/interim/<paper>_dwc_normalized.csv`, and `data/processed/<paper>_bien_staging.csv`.
5. Re-render the overview report: `Rscript -e "rmarkdown::render('reports/literature_data_overview.Rmd')"`.

---

## Repository Layout

- `config/` — `papers.csv` portfolio definition
- `mappings/` — paper-specific source-to-DWC/BIEN column mappings
- `scripts/` — workflow scripts (01–04 + `run_pipeline.R`)
  - `scripts/occurrence_intake/` — migrated DryadPlantTraits manual-intake scripts
- `data/raw/<paper_id>/` — downloaded source files (gitignored)
- `data/interim/` — discovered metadata + DwC-normalized tables
- `data/processed/` — BIEN staging outputs + per-paper summaries + habit sidecars
- `data/occurrences/` — migrated occurrence-intake compiled CSVs
- `data/occurrence_source_intake.csv` — 46-source registry (migrated from DryadPlantTraits)
- `logs/` — per-paper run logs and blocker diagnostics
- `reports/` — `literature_data_overview.Rmd` + rendered HTML (interactive map + per-paper tables)

---

## Reports

- `reports/literature_data_overview.html` — interactive overview with per-paper row counts, georeferenced map (leaflet), and pipeline-status tables. Re-render with:

```bash
Rscript -e "setwd('Literature_Data_To_BIENdb'); rmarkdown::render('reports/literature_data_overview.Rmd')"
```

---

## Provenance & Reproducibility

- Every paper run logs to `logs/<paper>.log` and `logs/discover_<paper>.log`.
- Per-paper `_staging_summary.csv` captures row counts and field coverage at staging time.
- All staging outputs preserve `source_file`, `source_sheet`, `original_row_number`, `sourceProvenance`, citation, and DOI for full traceback to the published source.
- Workspace-level `agents/agent_chat_provenance_log.txt` and `agents/prompt_log.md` record the prompts that drove each schema/script change.
