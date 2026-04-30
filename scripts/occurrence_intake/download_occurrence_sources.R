#!/usr/bin/env Rscript
# scripts/occurrence_intake/download_occurrence_sources.R
#
# Download and compile vascular plant occurrence records for 8 manually
# registered sources.  Outputs Darwin Core occurrence CSVs per source.
# Resumable: skips already-compiled outputs and already-downloaded raw files.

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(jsonlite)
})

# ---------------------------------------------------------------------------
# Locate project root
# ---------------------------------------------------------------------------
script_file <- tryCatch(normalizePath(sys.frame(0)$ofile, winslash = "/", mustWork = FALSE),
                        error = function(e) "")
if (!nzchar(script_file)) {
  args0 <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args0, value = TRUE)
  if (length(file_arg)) {
    script_file <- normalizePath(sub("^--file=", "", file_arg[[1]]),
                                 winslash = "/", mustWork = FALSE)
  }
}
root_candidates <- c(getwd(), dirname(getwd()),
                     dirname(dirname(getwd())), dirname(dirname(dirname(getwd()))))
if (nzchar(script_file)) {
  d1 <- dirname(script_file); d2 <- dirname(d1)
  d3 <- dirname(d2);         d4 <- dirname(d3)
  root_candidates <- c(root_candidates, d1, d2, d3, d4)
}
root_candidates <- unique(normalizePath(
  root_candidates[file.exists(root_candidates)], winslash = "/", mustWork = FALSE))
proj_hits <- root_candidates[basename(root_candidates) == "Literature_Data_To_BIENdb"]
if (!length(proj_hits)) stop("Cannot locate Literature_Data_To_BIENdb project root from: ", getwd(), call. = FALSE)
project_root <- proj_hits[[1]]
cat("Project root:", project_root, "\n")

# ---------------------------------------------------------------------------
# Output column schema (Darwin Core occurrences)
# ---------------------------------------------------------------------------
DWC_COLS <- c(
  "source_id", "occurrenceID", "species", "scientificName", "taxonRank",
  "decimalLatitude", "decimalLongitude", "coordinateUncertaintyInMeters",
  "countryCode", "country", "stateProvince", "locality", "eventDate",
  "year", "month", "day", "basisOfRecord", "institutionCode",
  "collectionCode", "catalogNumber", "recordedBy", "identifiedBy",
  "datasetName", "gbif_datasetKey", "source_doi",
  "download_timestamp_utc", "qa_flags"
)

empty_dwc <- function() {
  dt <- as.data.table(matrix(NA_character_, nrow = 0L, ncol = length(DWC_COLS)))
  setnames(dt, DWC_COLS)
  dt
}

ensure_dwc_cols <- function(dt, source_id_val, source_doi_val, ts, ds_key = NA_character_) {
  for (col in DWC_COLS) {
    if (!col %in% names(dt)) dt[, (col) := NA_character_]
  }
  if (!"source_id" %in% names(dt) || all(is.na(dt$source_id))) dt[, source_id := source_id_val]
  if (!"source_doi" %in% names(dt) || all(is.na(dt$source_doi))) dt[, source_doi := source_doi_val]
  if (!"download_timestamp_utc" %in% names(dt) || all(is.na(dt$download_timestamp_utc)))
    dt[, download_timestamp_utc := ts]
  if (!is.na(ds_key) && ("gbif_datasetKey" %in% names(dt)))
    dt[is.na(gbif_datasetKey), gbif_datasetKey := ds_key]
  dt[, .SD, .SDcols = DWC_COLS]
}

out_root <- file.path(project_root, "data", "occurrences")
dir.create(out_root, recursive = TRUE, showWarnings = FALSE)

TIMESTAMP <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
MAX_RECS  <- 10000L
PAGE_SIZE <- 300L

# ---------------------------------------------------------------------------
# Safe HTTP GET with retries
# ---------------------------------------------------------------------------
safe_get <- function(url, n_retry = 3L, pause = 2) {
  for (i in seq_len(n_retry)) {
    resp <- tryCatch(GET(url, timeout(60)), error = function(e) NULL)
    if (!is.null(resp) && status_code(resp) < 400) return(resp)
    Sys.sleep(pause)
  }
  NULL
}

# ---------------------------------------------------------------------------
# GBIF helpers
# ---------------------------------------------------------------------------
gbif_dataset_key <- function(doi) {
  # Resolve via doi.org redirect -> gbif.org/dataset/{uuid}
  doi_url <- paste0("https://doi.org/", doi)
  resp <- tryCatch(GET(doi_url, timeout(30)), error = function(e) NULL)
  if (!is.null(resp)) {
    final_url <- resp$url
    if (grepl("gbif\\.org/dataset/", final_url)) {
      key <- sub(".*gbif\\.org/dataset/", "", final_url)
      key <- sub("[/?#].*$", "", key)
      if (nzchar(key)) return(key)
    }
  }
  # Fallback: GBIF registry search
  encoded <- URLencode(doi, reserved = FALSE)
  url <- paste0("https://api.gbif.org/v1/dataset?doi=", encoded, "&limit=1")
  resp2 <- safe_get(url)
  if (is.null(resp2)) return(NA_character_)
  parsed <- tryCatch(fromJSON(rawToChar(resp2$content), simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(parsed) || length(parsed$results) == 0) return(NA_character_)
  # Only accept if the returned dataset's DOI actually matches
  res1 <- parsed$results[[1]]
  res_doi <- res1$doi
  if (!is.null(res_doi) && grepl(sub("^10\\.[0-9]+/", "", doi), res_doi, ignore.case = TRUE)) {
    return(res1$key)
  }
  NA_character_
}

gbif_fetch_page <- function(ds_key, offset) {
  url <- sprintf(
    "https://api.gbif.org/v1/occurrence/search?datasetKey=%s&limit=%d&offset=%d",
    ds_key, PAGE_SIZE, offset)
  resp <- safe_get(url)
  if (is.null(resp)) return(NULL)
  tryCatch(fromJSON(rawToChar(resp$content), simplifyVector = FALSE), error = function(e) NULL)
}

extract_field <- function(rec, field) {
  v <- rec[[field]]
  if (is.null(v)) return(NA_character_)
  as.character(v)
}

gbif_record_to_row <- function(rec, ds_key, source_doi_val, source_id_val) {
  data.table(
    source_id                        = source_id_val,
    occurrenceID                     = extract_field(rec, "key"),
    species                          = extract_field(rec, "species"),
    scientificName                   = extract_field(rec, "scientificName"),
    taxonRank                        = extract_field(rec, "taxonRank"),
    decimalLatitude                  = extract_field(rec, "decimalLatitude"),
    decimalLongitude                 = extract_field(rec, "decimalLongitude"),
    coordinateUncertaintyInMeters    = extract_field(rec, "coordinateUncertaintyInMeters"),
    countryCode                      = extract_field(rec, "countryCode"),
    country                          = extract_field(rec, "country"),
    stateProvince                    = extract_field(rec, "stateProvince"),
    locality                         = extract_field(rec, "locality"),
    eventDate                        = extract_field(rec, "eventDate"),
    year                             = extract_field(rec, "year"),
    month                            = extract_field(rec, "month"),
    day                              = extract_field(rec, "day"),
    basisOfRecord                    = extract_field(rec, "basisOfRecord"),
    institutionCode                  = extract_field(rec, "institutionCode"),
    collectionCode                   = extract_field(rec, "collectionCode"),
    catalogNumber                    = extract_field(rec, "catalogNumber"),
    recordedBy                       = extract_field(rec, "recordedBy"),
    identifiedBy                     = extract_field(rec, "identifiedBy"),
    datasetName                      = extract_field(rec, "datasetName"),
    gbif_datasetKey                  = ds_key,
    source_doi                       = source_doi_val,
    download_timestamp_utc           = TIMESTAMP,
    qa_flags                         = NA_character_
  )
}

ingest_gbif_source <- function(source_id_val, source_doi_val) {
  out_dir <- file.path(out_root, source_id_val)
  compiled <- file.path(out_dir, "compiled_occurrences.csv")
  if (file.exists(compiled)) {
    cat("  [SKIP] Already compiled:", compiled, "\n")
    dt <- fread(compiled, showProgress = FALSE)
    return(list(dt = dt, status = "skipped"))
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  cat("  Looking up GBIF dataset key for DOI:", source_doi_val, "\n")
  ds_key <- gbif_dataset_key(source_doi_val)
  if (is.na(ds_key)) {
    cat("  [ERROR] Could not resolve GBIF dataset key\n")
    return(list(dt = empty_dwc(), status = "error_key_lookup"))
  }
  cat("  Dataset key:", ds_key, "\n")

  # Count
  url_count <- sprintf("https://api.gbif.org/v1/occurrence/search?datasetKey=%s&limit=1", ds_key)
  resp_count <- safe_get(url_count)
  total_avail <- 0L
  if (!is.null(resp_count)) {
    parsed_count <- tryCatch(fromJSON(rawToChar(resp_count$content), simplifyVector = FALSE),
                             error = function(e) NULL)
    if (!is.null(parsed_count$count)) total_avail <- as.integer(parsed_count$count)
  }
  n_to_fetch <- min(total_avail, MAX_RECS)
  cat(sprintf("  Records available: %d  | fetching up to: %d\n", total_avail, n_to_fetch))

  rows <- list()
  offset <- 0L
  repeat {
    if (offset >= n_to_fetch) break
    cat(sprintf("    Page offset %d ...\n", offset))
    page <- gbif_fetch_page(ds_key, offset)
    if (is.null(page) || length(page$results) == 0) break
    for (rec in page$results) {
      rows[[length(rows) + 1L]] <- gbif_record_to_row(rec, ds_key, source_doi_val, source_id_val)
    }
    offset <- offset + PAGE_SIZE
    if (isTRUE(page$endOfRecords)) break
    Sys.sleep(1)
  }

  if (length(rows) == 0) {
    cat("  [WARN] No records fetched\n")
    dt <- empty_dwc()
  } else {
    dt <- rbindlist(rows, fill = TRUE, use.names = TRUE)
  }
  fwrite(dt, compiled)
  cat(sprintf("  Wrote %d rows -> %s\n", nrow(dt), compiled))
  list(dt = dt, status = "compiled")
}

# ---------------------------------------------------------------------------
# GBIF sources
# ---------------------------------------------------------------------------
gbif_sources <- list(
  list(source_id = "manual_gabon_gbif_ipt",          doi = "10.15468/apz3nj"),
  list(source_id = "manual_flora_sumatra_gbif_main",  doi = "10.15468/sncpxn"),
  list(source_id = "manual_flora_sumatra_gbif_anda2", doi = "10.15468/55evew"),
  list(source_id = "manual_flora_sumatra_batang_toru",doi = "10.15468/ue7xyn"),
  list(source_id = "manual_pucv_herbarium_gbif",      doi = "10.15468/k485f5")
)

results <- list()

for (src in gbif_sources) {
  cat("\n=== GBIF:", src$source_id, "===\n")
  res <- tryCatch(
    ingest_gbif_source(src$source_id, src$doi),
    error = function(e) { cat("  [ERROR]", conditionMessage(e), "\n"); list(dt = empty_dwc(), status = "error") }
  )
  results[[src$source_id]] <- list(nrow = nrow(res$dt), status = res$status)
}

# ---------------------------------------------------------------------------
# Zenodo: SIVFLORA
# ---------------------------------------------------------------------------
cat("\n=== Zenodo: manual_sivflora_zenodo ===\n")
sivflora_id   <- "manual_sivflora_zenodo"
sivflora_doi  <- "10.5281/zenodo.13997147"
sivflora_dir  <- file.path(out_root, sivflora_id)
sivflora_raw  <- file.path(sivflora_dir, "raw")
sivflora_out  <- file.path(sivflora_dir, "compiled_occurrences.csv")
dir.create(sivflora_raw, recursive = TRUE, showWarnings = FALSE)

sivflora_result <- tryCatch({
  if (file.exists(sivflora_out)) {
    cat("  [SKIP] Already compiled:", sivflora_out, "\n")
    dt <- fread(sivflora_out, showProgress = FALSE)
    results[[sivflora_id]] <- list(nrow = nrow(dt), status = "skipped")
  } else {
    # Get Zenodo record metadata
    zenodo_record_id <- "13997147"
    meta_url <- paste0("https://zenodo.org/api/records/", zenodo_record_id)
    cat("  Fetching Zenodo metadata:", meta_url, "\n")
    resp_meta <- safe_get(meta_url)
    if (is.null(resp_meta)) stop("Could not fetch Zenodo metadata")

    meta <- fromJSON(rawToChar(resp_meta$content), simplifyVector = FALSE)

    # Find files — may be under files, files.entries, or hits depending on Zenodo API version
    file_list <- NULL
    if (!is.null(meta$files)) file_list <- meta$files
    if (is.null(file_list) && !is.null(meta[["hits"]])) {
      # v2 search result
      if (length(meta$hits$hits) > 0) {
        file_list <- meta$hits$hits[[1]]$files
      }
    }
    # Zenodo legacy API: files are directly in meta$files as a list
    cat("  Files found:", length(file_list), "\n")

    # Pick first CSV or XLSX
    chosen_file <- NULL
    chosen_url  <- NULL
    for (f in file_list) {
      fname <- f[["key"]]
      if (is.null(fname)) fname <- f[["filename"]]
      if (is.null(fname)) next
      if (grepl("\\.csv$|\\.xlsx?$", fname, ignore.case = TRUE)) {
        chosen_file <- fname
        # link may be under links$self or links$download or directly as "links"
        lnk <- f[["links"]]
        if (!is.null(lnk[["self"]])) chosen_url <- lnk[["self"]]
        else if (!is.null(lnk[["download"]])) chosen_url <- lnk[["download"]]
        else if (!is.null(f[["self_html"]])) chosen_url <- f[["self_html"]]
        break
      }
    }
    if (is.null(chosen_url)) {
      # Try alternative: construct download URL from record files endpoint
      cat("  Trying files endpoint...\n")
      files_resp <- safe_get(paste0("https://zenodo.org/api/records/", zenodo_record_id, "/files"))
      if (!is.null(files_resp)) {
        files_meta <- fromJSON(rawToChar(files_resp$content), simplifyVector = FALSE)
        entries <- files_meta[["entries"]]
        if (is.null(entries)) entries <- files_meta[["files"]]
        for (f in entries) {
          fname <- f[["key"]]
          if (is.null(fname)) fname <- f[["id"]]
          if (is.null(fname)) next
          if (grepl("\\.csv$|\\.xlsx?$", fname, ignore.case = TRUE)) {
            chosen_file <- fname
            lnk <- f[["links"]]
            if (!is.null(lnk[["content"]])) chosen_url <- lnk[["content"]]
            else if (!is.null(lnk[["self"]])) chosen_url <- lnk[["self"]]
            break
          }
        }
      }
    }
    if (is.null(chosen_url)) stop("No CSV/XLSX file found in Zenodo record")
    cat("  Downloading:", chosen_file, "\n  URL:", chosen_url, "\n")

    dest <- file.path(sivflora_raw, chosen_file)
    if (!file.exists(dest)) {
      dl_resp <- GET(chosen_url, write_disk(dest, overwrite = FALSE), timeout(300))
      if (status_code(dl_resp) >= 400) stop("Download failed: HTTP ", status_code(dl_resp))
    } else {
      cat("  Raw file already exists, skipping download\n")
    }

    cat("  Reading:", dest, "\n")
    ext <- tolower(tools::file_ext(dest))
    if (ext == "csv") {
      raw_dt <- fread(dest, showProgress = FALSE, encoding = "UTF-8")
    } else {
      if (!requireNamespace("openxlsx", quietly = TRUE)) {
        install.packages("openxlsx", repos = "https://cloud.r-project.org", quiet = TRUE)
      }
      raw_dt <- as.data.table(openxlsx::read.xlsx(dest))
    }
    cat("  Raw dimensions:", nrow(raw_dt), "x", ncol(raw_dt), "\n")

    # Map to DwC: detect columns by pattern
    col_map <- function(dt, patterns, new_name) {
      hits <- grep(patterns, names(dt), ignore.case = TRUE, value = TRUE)
      if (length(hits) == 0) return(NA_character_)
      hits[1]
    }
    cn <- names(raw_dt)
    sci_col   <- col_map(raw_dt, "species|taxon|scientificname|name", "scientificName")
    lat_col   <- col_map(raw_dt, "^lat|decimallatitude|latitude", "decimalLatitude")
    lon_col   <- col_map(raw_dt, "^lon|decimallongitude|longitude", "decimalLongitude")
    cc_col    <- col_map(raw_dt, "countrycode|country_code|^cc$", "countryCode")
    ctry_col  <- col_map(raw_dt, "^country$|country_name", "country")
    occ_col   <- col_map(raw_dt, "occurrenceid|occurrence_id|id", "occurrenceID")
    yr_col    <- col_map(raw_dt, "^year$|event_year", "year")
    loc_col   <- col_map(raw_dt, "^locality$|location", "locality")
    isl_col   <- col_map(raw_dt, "island|site|plot", "locality")

    dt_out <- data.table(
      source_id                     = sivflora_id,
      occurrenceID                  = if (!is.na(occ_col)) as.character(raw_dt[[occ_col]]) else NA_character_,
      species                       = if (!is.na(sci_col)) as.character(raw_dt[[sci_col]]) else NA_character_,
      scientificName                = if (!is.na(sci_col)) as.character(raw_dt[[sci_col]]) else NA_character_,
      taxonRank                     = NA_character_,
      decimalLatitude               = if (!is.na(lat_col)) as.character(raw_dt[[lat_col]]) else NA_character_,
      decimalLongitude              = if (!is.na(lon_col)) as.character(raw_dt[[lon_col]]) else NA_character_,
      coordinateUncertaintyInMeters = NA_character_,
      countryCode                   = if (!is.na(cc_col)) as.character(raw_dt[[cc_col]]) else NA_character_,
      country                       = if (!is.na(ctry_col)) as.character(raw_dt[[ctry_col]]) else NA_character_,
      stateProvince                 = NA_character_,
      locality                      = if (!is.na(loc_col)) as.character(raw_dt[[loc_col]]) else
                                        if (!is.na(isl_col)) as.character(raw_dt[[isl_col]]) else NA_character_,
      eventDate                     = NA_character_,
      year                          = if (!is.na(yr_col)) as.character(raw_dt[[yr_col]]) else NA_character_,
      month                         = NA_character_,
      day                           = NA_character_,
      basisOfRecord                 = NA_character_,
      institutionCode               = NA_character_,
      collectionCode                = NA_character_,
      catalogNumber                 = NA_character_,
      recordedBy                    = NA_character_,
      identifiedBy                  = NA_character_,
      datasetName                   = "SIVFLORA",
      gbif_datasetKey               = NA_character_,
      source_doi                    = sivflora_doi,
      download_timestamp_utc        = TIMESTAMP,
      qa_flags                      = NA_character_
    )

    fwrite(dt_out, sivflora_out)
    cat(sprintf("  Wrote %d rows -> %s\n", nrow(dt_out), sivflora_out))
    results[[sivflora_id]] <- list(nrow = nrow(dt_out), status = "compiled")
  }
}, error = function(e) {
  cat("  [ERROR]", conditionMessage(e), "\n")
  results[[sivflora_id]] <<- list(nrow = 0L, status = paste0("error: ", conditionMessage(e)))
})

# ---------------------------------------------------------------------------
# Dryad helper: lookup version ID, download ZIP, extract CSVs
# ---------------------------------------------------------------------------
dryad_download_and_extract <- function(source_id_val, doi_str, out_dir) {
  raw_dir <- file.path(out_dir, "raw")
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

  encoded_doi <- URLencode(paste0("doi:", doi_str), reserved = TRUE)
  versions_url <- paste0("https://datadryad.org/api/v2/datasets/", encoded_doi, "/versions")
  cat("  Fetching Dryad versions:", versions_url, "\n")
  resp_ver <- safe_get(versions_url)
  if (is.null(resp_ver)) stop("Could not fetch Dryad versions")

  ver_parsed <- fromJSON(rawToChar(resp_ver$content), simplifyVector = FALSE)
  version_list <- ver_parsed[["_embedded"]][["stash:versions"]]
  if (is.null(version_list) || length(version_list) == 0) stop("No versions found")

  latest_ver <- version_list[[length(version_list)]]
  ver_id <- latest_ver[["id"]]
  if (is.null(ver_id)) {
    # fallback: extract from self link
    self_link <- latest_ver[["_links"]][["self"]][["href"]]
    ver_id <- basename(self_link)
  }
  cat("  Dryad version ID:", ver_id, "\n")

  zip_path <- file.path(raw_dir, paste0(source_id_val, "_dryad_version.zip"))
  if (!file.exists(zip_path)) {
    dl_url <- paste0("https://datadryad.org/api/v2/versions/", ver_id, "/download")
    cat("  Downloading ZIP:", dl_url, "\n")
    dl_resp <- GET(dl_url, write_disk(zip_path, overwrite = FALSE),
                   timeout(600), progress())
    if (status_code(dl_resp) >= 400) {
      file.remove(zip_path)
      stop("Dryad ZIP download failed: HTTP ", status_code(dl_resp))
    }
  } else {
    cat("  ZIP already exists, skipping download\n")
  }

  # Extract
  extract_dir <- file.path(raw_dir, "extracted")
  dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)
  cat("  Extracting ZIP ...\n")
  tryCatch(unzip(zip_path, exdir = extract_dir, overwrite = FALSE),
           error = function(e) cat("  [WARN] unzip error (partial extract may be OK):", conditionMessage(e), "\n"))

  # Find CSV/XLSX files
  csvs <- list.files(extract_dir, pattern = "\\.csv$|\\.xlsx?$", recursive = TRUE,
                     full.names = TRUE, ignore.case = TRUE)
  cat("  Found", length(csvs), "data files\n")
  csvs
}

read_data_file <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext == "csv") {
    dt <- tryCatch(fread(path, showProgress = FALSE, encoding = "UTF-8"),
                   error = function(e) {
                     fread(path, showProgress = FALSE, encoding = "Latin-1")
                   })
    return(dt)
  }
  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    install.packages("openxlsx", repos = "https://cloud.r-project.org", quiet = TRUE)
  }
  as.data.table(openxlsx::read.xlsx(path))
}

has_coord_cols <- function(dt) {
  cn <- tolower(names(dt))
  any(grepl("lat|decimallatitude", cn)) && any(grepl("lon|decimallongitude", cn))
}

dryad_map_dwc <- function(dt, source_id_val, doi_str, dataset_name_val) {
  cn <- names(dt)
  cn_lc <- tolower(cn)

  find_col <- function(patterns) {
    for (pat in patterns) {
      hits <- grep(pat, cn_lc, value = FALSE)
      if (length(hits) > 0) return(cn[hits[1]])
    }
    NA_character_
  }

  sci_col   <- find_col(c("scientificname", "species", "taxon", "name"))
  lat_col   <- find_col(c("decimallatitude", "^lat$", "latitude"))
  lon_col   <- find_col(c("decimallongitude", "^lon$", "^long$", "longitude"))
  cc_col    <- find_col(c("countrycode", "country_code"))
  ctry_col  <- find_col(c("^country$", "country_name"))
  occ_col   <- find_col(c("occurrenceid", "occurrence_id", "^id$", "gbifid"))
  sp_col    <- find_col(c("^species$"))
  rank_col  <- find_col(c("taxonrank", "rank"))
  edate_col <- find_col(c("eventdate", "event_date", "date"))
  yr_col    <- find_col(c("^year$"))
  mo_col    <- find_col(c("^month$"))
  dy_col    <- find_col(c("^day$"))
  bor_col   <- find_col(c("basisofrecord", "basis"))
  inst_col  <- find_col(c("institutioncode", "institution"))
  coll_col  <- find_col(c("collectioncode", "collection"))
  cat_col   <- find_col(c("catalognumber", "catalog"))
  rby_col   <- find_col(c("recordedby", "recorded_by", "collector"))
  iby_col   <- find_col(c("identifiedby", "identified_by", "determiner"))
  loc_col   <- find_col(c("locality", "location", "site"))
  state_col <- find_col(c("stateprovince", "state", "province", "region"))
  cunc_col  <- find_col(c("coordinateuncertainty", "uncertainty"))
  ds_col    <- find_col(c("datasetname", "dataset"))

  safe_col <- function(col) {
    if (is.na(col)) return(NA_character_)
    as.character(dt[[col]])
  }

  qa <- NA_character_
  if (is.na(lat_col) || is.na(lon_col)) qa <- "no_point_coordinates"

  data.table(
    source_id                     = source_id_val,
    occurrenceID                  = safe_col(occ_col),
    species                       = safe_col(sp_col),
    scientificName                = safe_col(sci_col),
    taxonRank                     = safe_col(rank_col),
    decimalLatitude               = safe_col(lat_col),
    decimalLongitude              = safe_col(lon_col),
    coordinateUncertaintyInMeters = safe_col(cunc_col),
    countryCode                   = safe_col(cc_col),
    country                       = safe_col(ctry_col),
    stateProvince                 = safe_col(state_col),
    locality                      = safe_col(loc_col),
    eventDate                     = safe_col(edate_col),
    year                          = safe_col(yr_col),
    month                         = safe_col(mo_col),
    day                           = safe_col(dy_col),
    basisOfRecord                 = safe_col(bor_col),
    institutionCode               = safe_col(inst_col),
    collectionCode                = safe_col(coll_col),
    catalogNumber                 = safe_col(cat_col),
    recordedBy                    = safe_col(rby_col),
    identifiedBy                  = safe_col(iby_col),
    datasetName                   = if (!is.na(ds_col)) safe_col(ds_col) else dataset_name_val,
    gbif_datasetKey               = NA_character_,
    source_doi                    = doi_str,
    download_timestamp_utc        = TIMESTAMP,
    qa_flags                      = qa
  )
}

ingest_dryad_source <- function(source_id_val, doi_str, dataset_name_val,
                                require_coords = FALSE) {
  out_dir  <- file.path(out_root, source_id_val)
  compiled <- file.path(out_dir, "compiled_occurrences.csv")
  if (file.exists(compiled)) {
    cat("  [SKIP] Already compiled:", compiled, "\n")
    dt <- fread(compiled, showProgress = FALSE)
    return(list(dt = dt, status = "skipped"))
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  data_files <- dryad_download_and_extract(source_id_val, doi_str, out_dir)

  if (length(data_files) == 0) {
    cat("  [WARN] No data files found after extraction\n")
    fwrite(empty_dwc(), compiled)
    return(list(dt = empty_dwc(), status = "no_files"))
  }

  all_parts <- list()
  for (f in data_files) {
    cat("  Reading:", basename(f), "\n")
    dt_raw <- tryCatch(read_data_file(f), error = function(e) {
      cat("    [WARN] Could not read:", conditionMessage(e), "\n"); NULL
    })
    if (is.null(dt_raw) || nrow(dt_raw) == 0) next
    if (require_coords && !has_coord_cols(dt_raw)) {
      cat("    No coordinate columns found, skipping file\n")
      next
    }
    part <- tryCatch(dryad_map_dwc(dt_raw, source_id_val, doi_str, dataset_name_val),
                     error = function(e) {
                       cat("    [WARN] Mapping error:", conditionMessage(e), "\n"); NULL
                     })
    if (!is.null(part)) all_parts[[length(all_parts) + 1L]] <- part
  }

  if (length(all_parts) == 0) {
    cat("  [WARN] No mappable data found\n")
    fwrite(empty_dwc(), compiled)
    return(list(dt = empty_dwc(), status = "no_data"))
  }

  dt_final <- rbindlist(all_parts, fill = TRUE, use.names = TRUE)
  fwrite(dt_final, compiled)
  cat(sprintf("  Wrote %d rows -> %s\n", nrow(dt_final), compiled))
  list(dt = dt_final, status = "compiled")
}

# ---------------------------------------------------------------------------
# Kyrgyzstan Dryad 2025 (DwC-ready)
# ---------------------------------------------------------------------------
cat("\n=== Dryad: manual_kyrgyzstan_dryad_2025 ===\n")
res_kyrg <- tryCatch(
  ingest_dryad_source(
    source_id_val   = "manual_kyrgyzstan_dryad_2025",
    doi_str         = "10.5061/dryad.x3ffbg7wt",
    dataset_name_val = "Kyrgyzstan vascular plants (Sennikov & Lazkov 2025)",
    require_coords  = FALSE
  ),
  error = function(e) {
    cat("  [ERROR]", conditionMessage(e), "\n")
    list(dt = empty_dwc(), status = paste0("error: ", conditionMessage(e)))
  }
)
results[["manual_kyrgyzstan_dryad_2025"]] <- list(nrow = nrow(res_kyrg$dt), status = res_kyrg$status)

# ---------------------------------------------------------------------------
# PacIFlora Dryad
# ---------------------------------------------------------------------------
cat("\n=== Dryad: manual_paciflora_dryad ===\n")
res_pac <- tryCatch(
  ingest_dryad_source(
    source_id_val    = "manual_paciflora_dryad",
    doi_str          = "10.5061/dryad.qfttdz0hd",
    dataset_name_val = "PacIFlora — Pacific Introduced Flora (Wohlwend et al. 2021)",
    require_coords   = FALSE
  ),
  error = function(e) {
    cat("  [ERROR]", conditionMessage(e), "\n")
    list(dt = empty_dwc(), status = paste0("error: ", conditionMessage(e)))
  }
)
results[["manual_paciflora_dryad"]] <- list(nrow = nrow(res_pac$dt), status = res_pac$status)

# ---------------------------------------------------------------------------
# TASK 3 — Combine all compiled outputs
# ---------------------------------------------------------------------------
cat("\n=== Combining all compiled occurrence files ===\n")

all_compiled_files <- list.files(out_root, pattern = "compiled_occurrences\\.csv$",
                                 recursive = TRUE, full.names = TRUE)
# Exclude the combined output itself if it already exists
all_source_files <- all_compiled_files[!grepl("compiled_occurrences_all\\.csv$", all_compiled_files)]

cat("  Found", length(all_source_files), "per-source compiled files\n")

combined_parts <- lapply(all_source_files, function(f) {
  tryCatch({
    dt <- fread(f, showProgress = FALSE)
    if (!"source_id" %in% names(dt) || all(is.na(dt$source_id))) {
      dt[, source_id := basename(dirname(f))]
    }
    dt
  }, error = function(e) {
    cat("  [WARN] Could not read:", f, "-", conditionMessage(e), "\n")
    NULL
  })
})
combined_parts <- Filter(Negate(is.null), combined_parts)

combined_all <- if (length(combined_parts) > 0) {
  rbindlist(combined_parts, fill = TRUE, use.names = TRUE)
} else {
  empty_dwc()
}

combined_out <- file.path(out_root, "compiled_occurrences_all.csv")
fwrite(combined_all, combined_out)
cat(sprintf("  Combined total: %d rows -> %s\n", nrow(combined_all), combined_out))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat("\n=== Summary by source ===\n")
for (sid in names(results)) {
  r <- results[[sid]]
  cat(sprintf("  %-45s  %6d rows  [%s]\n", sid, r$nrow, r$status))
}

if (nrow(combined_all) > 0) {
  n_species <- uniqueN(combined_all[!is.na(scientificName) & scientificName != "", scientificName])
  cat(sprintf("\n  Total rows (all sources): %d\n", nrow(combined_all)))
  cat(sprintf("  Unique scientificName values: %d\n", n_species))
}

# ---------------------------------------------------------------------------
# TASK 4 — Update harvest_status in occurrence_source_intake.csv
# ---------------------------------------------------------------------------
cat("\n=== Updating harvest_status in occurrence_source_intake.csv ===\n")

intake_path <- file.path(project_root, "data", "occurrence_source_intake.csv")
intake <- fread(intake_path, showProgress = FALSE)

compiled_ids <- names(Filter(function(r) r$nrow > 0 && !grepl("^error", r$status),
                             results))
cat("  Sources to mark compiled:", paste(compiled_ids, collapse = ", "), "\n")

intake[source_id %in% compiled_ids & harvest_status == "pending_review",
       harvest_status := "compiled"]

fwrite(intake, intake_path)
cat("  Updated", intake_path, "\n")

cat("\nDone.\n")
