# Literature_Data_To_BIENdb Chat Provenance Log

## 2026-04-30 - Add per-source quality stats and leaflet map to progress report

**Prompt:** Update source_data_progress_report.Rmd with new sections: per-source quality summary (georef, binomial) and leaflet occurrence map; re-render HTML; commit and push.

**Summary:**
- Replaced the simple `compiled_rows` vapply block in the setup chunk with a single-pass `read_source_summary()` function (tryCatch-guarded) that reads each compiled CSV once and extracts total_records, georef_records, and binomial_records.
- Added `georef_records`, `binomial_records`, `pct_georef`, `pct_binomial` columns to `intake_enriched`.
- Built `all_georef_pts` data frame (all georeferenced points across compiled sources, capped at 50,000 random rows with `set.seed(42)`) for the leaflet map.
- Added `library(leaflet)` to the library block.
- Added new Section 3 "Compiled Source Quality Summary" containing: (a) a kable table with per-source and totals row; (b) subsection "Geographic Distribution of Compiled Occurrences" with a leaflet map colored by `source_id` using `colorFactor("Set1")`, circle markers with popup (`source_id` + `species`), and a bottom-right legend.
- Remaining sections renumbered (Sources Pending → 4, What Has Been Done → 5, What Remains → 6, Reproducibility → 7).
- Rendered `source_data_progress_report.html` successfully (no errors).
- Committed and pushed to origin.

## 2026-04-28 - Mapping parse resilience for normalization

**Prompt:** Patch `scripts/03_normalize_to_dwc.R` so malformed mapping CSVs cannot hard-fail normalization: wrap mapping read in `tryCatch`, validate required columns (at least `source_column`) before dereference, and warn + continue with heuristic extraction on mapping parse/column issues; then parse-check and run `jennings_2026` pipeline.

**Summary:**
- Updated mapping ingest in `scripts/03_normalize_to_dwc.R` to treat mapping read/shape issues as non-fatal.
- Added `tryCatch` around `read.csv(...)` with concise warning log output on parse/read errors.
- Added required-column validation for `source_column` before normalization of mapping fields; missing columns now log warning and trigger heuristic-only continuation.
- Preserved existing behavior when mapping is valid, including informational load logging.
- Ran requested syntax parse check and re-ran pipeline for `jennings_2026` after patch.

## 2026-04-28 - Targeted code-checker fixes for Jennings pipeline

**Prompt:** Apply targeted fixes in `Literature_Data_To_BIENdb` for sidecar alignment, zero-row habit sidecar schema, GNRS political bundle scope, mapping-file behavior, coordinate sanity checks for x/y fallback, and README CLI consistency; then parse scripts, rerun `jennings_2026`, and report key field counts.

**Summary:**
- Fixed `scripts/04_build_bien_staging.R` to compute habit sidecar rows from the already-filtered/aligned staging rows, eliminating positional mismatch between normalized input and staging output.
- Replaced the no-habit placeholder record with a true zero-row sidecar CSV preserving schema.
- Narrowed `politicalUnitsRaw` composition in `scripts/03_normalize_to_dwc.R` to political units only (`country`, `countryCode`, `stateProvince`, `county`, `municipality`).
- Made mapping file loading non-brittle in `scripts/03_normalize_to_dwc.R`: if missing, pipeline now logs a warning and proceeds with heuristic extraction; if present, logs informational load status.
- Added coordinate sanity checks in normalization so lat/lon outside valid ranges are set to `NA`, while raw coordinate text is retained in `verbatimLatitude`/`verbatimLongitude`.
- Updated README CLI documentation to match current `run_pipeline.R` arguments and documented zero-row habit sidecar behavior.
- Re-validated script parsing and re-ran pipeline for `jennings_2026` with post-run field-count checks.

## 2026-04-28 - Jennings 2026 richer occurrence extraction, GNRS fields, and habit sidecar

**Prompt:** Update the Jennings 2026 pipeline to extract richer occurrence fields for BIEN ViewFullOccurrence alignment, include GNRS political-unit-ready fields, separate habit/growth-form trait metadata into its own output, update README mapping notes, rerun pipeline outputs, and report key non-missing counts.

**Summary:**
- Inspected Jennings source workbooks and existing normalization/staging outputs to align extraction strictly to available source columns.
- Expanded normalization (`scripts/03_normalize_to_dwc.R`) with broader occurrence extraction: deterministic fallback `occurrenceID`, `recordNumber`, expanded locality/political fields (`countryCode`, `stateProvince`, `county`, `municipality`, `islandGroup`, `waterBody`, raw political text), georeference fields, event year/month/day, and source provenance columns.
- Preserved/expanded BIEN staging (`scripts/04_build_bien_staging.R`) to include GNRS input-ready geography columns plus raw political text columns for traceability.
- Added separate habit trait sidecar output `data/processed/jennings_2026_habit_traits.csv` with growth-form rows when present, and explicit NA placeholder behavior/logging when absent.
- Updated README with extraction design notes and field mapping behavior for occurrence, GNRS, and trait sidecar outputs.
- Re-ran Jennings pipeline steps to regenerate outputs with the updated schema and prepared key field completeness counts.

## 2026-04-28 - Initial scaffold and Jennings 2026 bootstrap

**Prompt:** Implement a new project folder `Literature_Data_To_BIENdb` to start a literature-to-BIEN ingestion workflow, bootstrap with Jennings et al. 2026 (PhytoKeys), discover/download dataset links, map to Darwin Core-like and BIEN staging outputs, and run once with logs.

**Summary:**
- Created project scaffold (`config`, `mappings`, `scripts`, `data/raw`, `data/interim`, `data/processed`, `logs`).
- Added paper-specific config for DOI `10.3897/phytokeys.273.184780` and paper-specific mapping file.
- Implemented four executable R pipeline steps plus a top-level orchestrator.
- Implemented discovery against DOI/Crossref/article XML and supplementary-material extraction.
- Implemented idempotent downloads with blocker logging.
- Implemented normalization to Darwin Core-like table and BIEN staging CSV output.
- Ran pipeline once and captured outputs/logs.

## 2026-04-28 - GitHub repository linkage and scoped push

**Prompt:** There is an existing GitHub repository at `https://github.com/benquist/Literature_Data_To_BIENdb`; verify local folder state, initialize/configure git if needed, commit scaffold files in this project only, push to origin default branch, and update required provenance logs.

**Summary:**
- Verified `/Users/brianjenquist/VSCode/Literature_Data_To_BIENdb` existed and was not yet an independent git repo (`NO_DOT_GIT`).
- Initialized git in the project folder, configured `origin` to `https://github.com/benquist/Literature_Data_To_BIENdb`, and fetched remote history.
- Detected existing remote `main` history; safely synchronized by temporarily moving local scaffold files, pulling `origin/main`, then restoring scaffold files for commit.
- Committed project scaffold/provenance/log files in the project repo scope only and pushed to `origin/main`.

## 2026-04-28 - Jennings 2026 restart for coordinates/elevation

**Prompt:** We forgot latitude and longitude for each observation as well as elevation. I think we need to start over.

**Summary:**
- Audited current Jennings 2026 normalized and BIEN staging outputs and confirmed schema gaps for explicit elevation fields and limited source-column matching for coordinate/elevation variants.
- Updated normalization parser to accept common coordinate variants (`lat`, `latitude`, `lon`, `long`, `lng`) and elevation variants (`elev`, `elevation`, `elevation_m`, `alt`, `altitude`) while retaining Darwin Core output columns.
- Added explicit normalized output columns `verbatimElevation` and `elevation` and ensured elevation range derivation behavior preserves `NA` when unavailable.
- Updated BIEN staging builder to emit explicit `decimalLatitude`, `decimalLongitude`, `verbatim_elevation`, and `elevation_m` fields in addition to existing staging coordinate/elevation fields.
- Re-ran pipeline from source with force overwrite for `jennings_2026`, regenerated interim and processed outputs, and validated coordinate/elevation columns and non-missing counts.
- Updated README with brief coordinate/elevation mapping behavior and force-rebuild expectations.

## 2026-04-29 - Batch onboarding and multi-paper processing kickoff

**Prompt:** Continue project work after morning status check; commit `jennings_2026`, register queued papers, prioritize machine-readable occurrence datasets, and start processing.

**Summary:**
- Committed and pushed finalized `jennings_2026` outputs and script hardening updates to `origin/main`.
- Added 10 queued papers to `config/papers.csv` with DOI/publisher metadata and initial source notes.
- Added GBIF DwC-A ZIP ingestion support to `scripts/03_normalize_to_dwc.R` by extracting and parsing `occurrence.txt` directly from archives.
- Fixed normalization edge cases introduced by column-name normalization: scientific-name extraction now supports lowercase and checklist-style name columns (`name`, `accepted_species`, `accepted_name`) and event date extraction now handles normalized `eventdate`.
- Fixed discovery robustness in `scripts/01_discover_paper_assets.R` for zero-asset XML cases (no-row schema + safe metadata assignment).
- Downloaded and processed GBIF/IPT DwC-A datasets for `gosline_2023`, `novikov_2022`, and `dayneko_2023` through staging.
- Ran discovery/download/normalize/staging for `joyce_2020` and `aung_2025` using supplementary XLSX assets.
- New staged outputs generated for: `gosline_2023`, `novikov_2022`, `dayneko_2023`, `joyce_2020`, and `aung_2025`.

## 2026-04-29 - Manual occurrence migration finalized from Dryad trait intake

**Prompt:** Complete migration of clearly non-trait pending literature sources into Literature_Data_To_BIENdb, ensure occurrence registry totals and moved scripts are complete, and align script registry/output paths with Literature project layout.

**Summary:**
- Confirmed `data/occurrence_source_intake.csv` contains 46 rows with status distribution: compiled 10, pending_review 33, pending_manual_access 3.
- Confirmed moved occurrence files are present under `data/occurrences/` and script set under `scripts/occurrence_intake/`.
- Patched `scripts/occurrence_intake/download_occurrence_sources.R` so harvest-status writeback updates `data/occurrence_source_intake.csv` (not Dryad manual intake path).
- Prepared this project scope for commit/push to `origin/main`.

## 2026-04-29 - README occurrence-program status addition for project split

**Prompt:** Update README.md so Literature_Data_To_BIENdb explicitly carries migrated occurrence intake program status from DryadPlantTraits, including registry counts, compiled row/georeference totals, and script/data locations.

**Summary:**
- Added a concise Pipeline Status subsection for migrated occurrence intake program.
- Documented registry state: 46 total sources (10 compiled, 33 pending_review, 3 pending_manual_access).
- Documented compiled occurrence totals: 165,155 rows and 144,389 georeferenced.
- Documented canonical locations: scripts/occurrence_intake/, data/occurrences/, and data/occurrence_source_intake.csv.

## 2026-04-30 - Source data progress report and waiting list

**Prompt:** For Literature_Data_To_BIENdb, provide a full summary of all data sources accessed for literature data integration, what has been done, what remains to be done, and create a `.Rmd` + `.html` report for source data progress and pending waiting list.

**Summary:**
- Added `source_data_progress_report.Rmd` at project root.
- Report reads `data/occurrence_source_intake.csv` and computes accessed/ingested vs pending status directly from `harvest_status`.
- Added evidence fields from repository artifacts: compiled file existence and compiled row counts from `data/occurrences/<source_id>/compiled_occurrences.csv`.
- Included executive summary, accessed table, pending waiting-list table, done summary, actionable next-step checklist, timestamp, and reproducibility notes.
- Rendered `source_data_progress_report.html` successfully via `rmarkdown::render`.

## 2026-04-30 — Add Literature Paper Integration Status section to source progress report

- Updated `source_data_progress_report.Rmd`: added literature papers setup code in the setup chunk (reads `config/papers.csv`, loads all `data/processed/<paper_id>_staging_summary.csv` files that exist, builds `papers_all` join, loads `lit_georef_pts` from `*_bien_staging.csv` files).
- Added new top-level section `# Literature Paper Integration Status` before the existing Executive Summary, containing:
  - Executive sub-summary (inline R: total/processed/pending papers, total staging rows, total georef pts).
  - DT table of all 11 papers with DOI hyperlinks, status (Processed/Pending), and quality metrics.
  - Leaflet map (`CartoDB.Positron`) of georeferenced literature paper points colored by paper_id (max 50k sample).
- Processed papers (staging_summary exists): jennings_2026, gosline_2023, wasowicz_2020, novikov_2022, dayneko_2023, joyce_2020, aung_2025 (7 of 11).
- Rendered `source_data_progress_report.html` successfully without errors.
