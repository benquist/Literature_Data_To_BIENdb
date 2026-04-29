# Literature_Data_To_BIENdb Chat Provenance Log

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
