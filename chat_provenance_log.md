# Literature_Data_To_BIENdb Chat Provenance Log

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
