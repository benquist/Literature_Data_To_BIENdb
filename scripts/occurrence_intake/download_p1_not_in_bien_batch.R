#!/usr/bin/env Rscript
# scripts/occurrence_intake/download_p1_not_in_bien_batch.R
#
# Ingest 5 P1 / not-in-BIEN manual occurrence sources.
# Resumable: skips sources whose compiled_occurrences.csv already exists.
# Outputs Darwin Core occurrence CSVs per source to:
#   data/occurrences/<source_id>/compiled_occurrences.csv

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

placeholder_dwc <- function(source_id_val, source_doi_val, qa_flag_val, ts) {
  empty_dwc()
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
  encoded <- URLencode(doi, reserved = FALSE)
  url <- paste0("https://api.gbif.org/v1/dataset?doi=", encoded, "&limit=1")
  resp2 <- safe_get(url)
  if (is.null(resp2)) return(NA_character_)
  parsed <- tryCatch(fromJSON(rawToChar(resp2$content), simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(parsed) || length(parsed$results) == 0) return(NA_character_)
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

ingest_from_gbif_key <- function(source_id_val, source_doi_val, ds_key) {
  url_count <- sprintf("https://api.gbif.org/v1/occurrence/search?datasetKey=%s&limit=1", ds_key)
  resp_count <- safe_get(url_count)
  total_avail <- 0L
  if (!is.null(resp_count)) {
    parsed_count <- tryCatch(fromJSON(rawToChar(resp_count$content), simplifyVector = FALSE),
                             error = function(e) NULL)
    if (!is.null(parsed_count$count)) total_avail <- as.integer(parsed_count$count)
  }
  n_to_fetch <- min(total_avail, MAX_RECS)
  cat(sprintf("  GBIF records available: %d  | fetching up to: %d\n", total_avail, n_to_fetch))

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
  if (length(rows) == 0) return(NULL)
  rbindlist(rows, fill = TRUE, use.names = TRUE)
}

# ---------------------------------------------------------------------------
# Generic column mapper for CSV/TSV downloads
# ---------------------------------------------------------------------------
find_col <- function(cn, patterns) {
  cn_lc <- tolower(cn)
  for (pat in patterns) {
    hits <- grep(pat, cn_lc, value = FALSE)
    if (length(hits) > 0) return(cn[hits[1]])
  }
  NA_character_
}

map_to_dwc <- function(dt, source_id_val, source_doi_val, dataset_name_val,
                       bor = NA_character_, qa = NA_character_) {
  cn <- names(dt)
  sci_col   <- find_col(cn, c("scientificname", "species", "taxon", "name"))
  sp_col    <- find_col(cn, c("^species$"))
  lat_col   <- find_col(cn, c("decimallatitude", "^lat$", "latitude", "^y$"))
  lon_col   <- find_col(cn, c("decimallongitude", "^lon$", "^long$", "longitude", "^x$"))
  occ_col   <- find_col(cn, c("occurrenceid", "occurrence_id", "^id$", "plot_id", "plotid"))
  rank_col  <- find_col(cn, c("taxonrank", "rank"))
  edate_col <- find_col(cn, c("eventdate", "event_date", "date"))
  yr_col    <- find_col(cn, c("^year$", "event_year"))
  mo_col    <- find_col(cn, c("^month$"))
  dy_col    <- find_col(cn, c("^day$"))
  bor_col   <- find_col(cn, c("basisofrecord", "basis"))
  inst_col  <- find_col(cn, c("institutioncode", "institution"))
  coll_col  <- find_col(cn, c("collectioncode", "collection"))
  cat_col   <- find_col(cn, c("catalognumber", "catalog"))
  rby_col   <- find_col(cn, c("recordedby", "recorded_by", "collector", "observer"))
  iby_col   <- find_col(cn, c("identifiedby", "identified_by", "determiner"))
  loc_col   <- find_col(cn, c("locality", "location", "site", "plot", "placename"))
  state_col <- find_col(cn, c("stateprovince", "state", "province", "region"))
  cc_col    <- find_col(cn, c("countrycode", "country_code"))
  ctry_col  <- find_col(cn, c("^country$", "country_name"))
  cunc_col  <- find_col(cn, c("coordinateuncertainty", "uncertainty", "coord_uncertainty"))
  ds_col    <- find_col(cn, c("datasetname", "dataset"))
  notes_col <- find_col(cn, c("notes", "remarks", "comments"))

  safe_col <- function(col) {
    if (is.na(col)) return(NA_character_)
    as.character(dt[[col]])
  }

  qa_out <- qa
  if (is.na(lat_col) || is.na(lon_col)) {
    qa_out <- if (!is.na(qa_out)) paste0(qa_out, "|no_point_coordinates") else "no_point_coordinates"
  }

  bor_val <- if (!is.na(bor_col)) safe_col(bor_col) else bor
  loc_val <- safe_col(loc_col)
  if (all(is.na(loc_val)) && !is.na(notes_col)) loc_val <- safe_col(notes_col)

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
    locality                      = loc_val,
    eventDate                     = safe_col(edate_col),
    year                          = safe_col(yr_col),
    month                         = safe_col(mo_col),
    day                           = safe_col(dy_col),
    basisOfRecord                 = bor_val,
    institutionCode               = safe_col(inst_col),
    collectionCode                = safe_col(coll_col),
    catalogNumber                 = safe_col(cat_col),
    recordedBy                    = safe_col(rby_col),
    identifiedBy                  = safe_col(iby_col),
    datasetName                   = if (!is.na(ds_col)) safe_col(ds_col) else dataset_name_val,
    gbif_datasetKey               = NA_character_,
    source_doi                    = source_doi_val,
    download_timestamp_utc        = TIMESTAMP,
    qa_flags                      = qa_out
  )
}

# ---------------------------------------------------------------------------
# Crossref relation helper — find data deposit DOI from paper DOI
# ---------------------------------------------------------------------------
crossref_find_data_doi <- function(paper_doi) {
  url <- paste0("https://api.crossref.org/works/", URLencode(paper_doi, reserved = FALSE))
  cat("  Crossref lookup:", url, "\n")
  resp <- safe_get(url)
  if (is.null(resp)) return(NA_character_)
  parsed <- tryCatch(fromJSON(rawToChar(resp$content), simplifyVector = FALSE),
                     error = function(e) NULL)
  if (is.null(parsed)) return(NA_character_)
  msg <- parsed[["message"]]
  if (is.null(msg)) return(NA_character_)

  # Check relation field
  rel <- msg[["relation"]]
  if (!is.null(rel)) {
    for (rel_type in names(rel)) {
      items <- rel[[rel_type]]
      for (item in items) {
        id_type <- item[["id-type"]]
        id_val  <- item[["id"]]
        if (!is.null(id_type) && !is.null(id_val) &&
            tolower(id_type) == "doi" &&
            grepl("zenodo|dryad|figshare|osf", id_val, ignore.case = TRUE)) {
          cat("  Found related data DOI:", id_val, "(relation:", rel_type, ")\n")
          return(id_val)
        }
      }
    }
  }

  # Check link array for supplementary or data URLs
  links <- msg[["link"]]
  if (!is.null(links)) {
    for (lnk in links) {
      url_val <- lnk[["URL"]]
      if (!is.null(url_val) &&
          grepl("zenodo|dryad|figshare|datadryad|osf\\.io", url_val, ignore.case = TRUE)) {
        cat("  Found data link in crossref links:", url_val, "\n")
        return(url_val)
      }
    }
  }

  NA_character_
}

# ---------------------------------------------------------------------------
# Zenodo search helper
# ---------------------------------------------------------------------------
zenodo_search_first_csv_url <- function(query_str) {
  enc_q <- URLencode(query_str, reserved = FALSE)
  url <- paste0("https://zenodo.org/api/records?q=", enc_q, "&type=dataset&sort=mostrecent")
  cat("  Zenodo search:", url, "\n")
  resp <- safe_get(url)
  if (is.null(resp)) return(list(url = NA_character_, record_doi = NA_character_))
  parsed <- tryCatch(fromJSON(rawToChar(resp$content), simplifyVector = FALSE),
                     error = function(e) NULL)
  if (is.null(parsed)) return(list(url = NA_character_, record_doi = NA_character_))

  hits <- parsed[["hits"]][["hits"]]
  if (is.null(hits) || length(hits) == 0)
    return(list(url = NA_character_, record_doi = NA_character_))

  for (hit in hits) {
    record_doi <- hit[["doi"]]
    file_list  <- hit[["files"]]
    if (is.null(file_list)) next
    for (f in file_list) {
      fname <- f[["key"]]
      if (is.null(fname)) fname <- f[["filename"]]
      if (is.null(fname)) next
      if (grepl("\\.csv$|\\.tsv$|\\.xlsx?$|\\.zip$", fname, ignore.case = TRUE)) {
        lnk <- f[["links"]]
        dl_url <- NULL
        if (!is.null(lnk[["self"]]))     dl_url <- lnk[["self"]]
        else if (!is.null(lnk[["download"]])) dl_url <- lnk[["download"]]
        if (!is.null(dl_url)) {
          cat("  Zenodo hit:", fname, "DOI:", record_doi, "\n")
          return(list(url = dl_url, fname = fname, record_doi = record_doi))
        }
      }
    }
  }
  list(url = NA_character_, record_doi = NA_character_)
}

# ---------------------------------------------------------------------------
# Dryad search helper
# ---------------------------------------------------------------------------
dryad_search_first_result <- function(query_str) {
  enc_q <- URLencode(query_str, reserved = FALSE)
  url <- paste0("https://datadryad.org/api/v2/search?q=", enc_q, "&per_page=5")
  cat("  Dryad search:", url, "\n")
  resp <- safe_get(url)
  if (is.null(resp)) return(NA_character_)
  parsed <- tryCatch(fromJSON(rawToChar(resp$content), simplifyVector = FALSE),
                     error = function(e) NULL)
  if (is.null(parsed)) return(NA_character_)
  embedded <- parsed[["_embedded"]]
  if (is.null(embedded)) return(NA_character_)
  items <- embedded[["stash:datasets"]]
  if (is.null(items) || length(items) == 0) return(NA_character_)
  doi_val <- items[[1]][["identifier"]]
  if (!is.null(doi_val)) {
    doi_val <- sub("^doi:", "", doi_val, ignore.case = TRUE)
    cat("  Dryad search hit DOI:", doi_val, "\n")
    return(doi_val)
  }
  NA_character_
}

# ---------------------------------------------------------------------------
# Generic: download file and read as data.table
# ---------------------------------------------------------------------------
download_and_read <- function(url, dest_path) {
  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)
  if (!file.exists(dest_path)) {
    cat("  Downloading:", url, "\n  -> ", dest_path, "\n")
    resp <- GET(url, write_disk(dest_path, overwrite = FALSE), timeout(90))
    if (status_code(resp) >= 400) {
      file.remove(dest_path)
      stop("HTTP ", status_code(resp), " for ", url)
    }
  } else {
    cat("  File already exists:", dest_path, "\n")
  }
  ext <- tolower(tools::file_ext(dest_path))
  if (ext %in% c("zip")) {
    extract_dir <- paste0(dest_path, "_extracted")
    dir.create(extract_dir, showWarnings = FALSE)
    tryCatch(unzip(dest_path, exdir = extract_dir, overwrite = FALSE),
             warning = function(w) NULL, error = function(e) NULL)
    csvs <- list.files(extract_dir, pattern = "\\.csv$|\\.tsv$|\\.xlsx?$",
                       recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
    if (length(csvs) == 0) return(NULL)
    cat("  ZIP extracted, reading:", basename(csvs[1]), "\n")
    dest_path <- csvs[1]
    ext <- tolower(tools::file_ext(dest_path))
  }
  if (ext == "csv") {
    return(tryCatch(fread(dest_path, showProgress = FALSE, encoding = "UTF-8"),
                    error = function(e)
                      tryCatch(fread(dest_path, showProgress = FALSE, encoding = "Latin-1"),
                               error = function(e2) NULL)))
  }
  if (ext == "tsv") {
    return(tryCatch(fread(dest_path, showProgress = FALSE, sep = "\t", encoding = "UTF-8"),
                    error = function(e) NULL))
  }
  NULL
}

# ---------------------------------------------------------------------------
# Write compiled output (never overwrites non-empty file with 0 rows)
# ---------------------------------------------------------------------------
safe_write_compiled <- function(dt, compiled_path) {
  if (file.exists(compiled_path)) {
    existing <- tryCatch(fread(compiled_path, showProgress = FALSE), error = function(e) NULL)
    if (!is.null(existing) && nrow(existing) > 0 && nrow(dt) == 0) {
      cat("  [SKIP-WRITE] Refusing to overwrite", nrow(existing),
          "existing rows with 0 rows\n")
      return(invisible(FALSE))
    }
  }
  dir.create(dirname(compiled_path), recursive = TRUE, showWarnings = FALSE)
  fwrite(dt, compiled_path)
  cat(sprintf("  Wrote %d rows -> %s\n", nrow(dt), compiled_path))
  invisible(TRUE)
}

results <- list()

`%||%` <- function(a, b) if (!is.null(a)) a else b

# ===========================================================================
# Source 1: manual_arroyo_high_andes_chile
# DOI: 10.17632/8zvjvcyv79.1 (Mendeley Data)
# Strategy: direct Mendeley XLSX download -> Python openpyxl conversion -> DWC mapping
# ===========================================================================
cat("\n=== Source 1: manual_arroyo_high_andes_chile ===\n")
s1_id    <- "manual_arroyo_high_andes_chile"
s1_doi   <- "10.17632/8zvjvcyv79.1"
s1_dir   <- file.path(out_root, s1_id)
s1_out   <- file.path(s1_dir, "compiled_occurrences.csv")
s1_url   <- "https://data.mendeley.com/public-files/datasets/8zvjvcyv79/files/84bb7046-b6e9-4f99-8f94-1ac3b08dde7b/file_downloaded"

results[[s1_id]] <- tryCatch({
  skip_result <- NULL
  if (file.exists(s1_out)) {
    dt <- fread(s1_out, showProgress = FALSE)
    if (nrow(dt) > 0) {
      cat("  [SKIP] Already compiled with non-empty output:", s1_out, "\n")
      skip_result <- list(nrow = nrow(dt), status = "skipped")
    } else {
      cat("  Existing output is empty; rebuilding from source data\n")
      file.remove(s1_out)
    }
  }
  if (!is.null(skip_result)) {
    skip_result
  } else {
    dir.create(s1_dir, recursive = TRUE, showWarnings = FALSE)
    raw_dir <- file.path(s1_dir, "raw")
    dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
    xlsx_path <- file.path(raw_dir, "arroyo_mendeley.xlsx")
    csv_path <- file.path(raw_dir, "arroyo_mendeley_occurrences.csv")

    cat("  Downloading Mendeley XLSX...\n")
    resp <- GET(s1_url, write_disk(xlsx_path, overwrite = TRUE), timeout(120))
    if (status_code(resp) >= 400) {
      stop("Failed to download Mendeley XLSX. HTTP status ", status_code(resp))
    }

    py_script <- tempfile(fileext = ".py")
    py_code <- c(
      "import openpyxl, csv, re, sys",
      "xlsx_path = sys.argv[1]",
      "out_path = sys.argv[2]",
      "wb = openpyxl.load_workbook(xlsx_path)",
      "ws_meta = wb['Plot_metadata']",
      "rows_meta = list(ws_meta.iter_rows(values_only=True))",
      "def dms_to_dd(dms_str):",
      "    if not dms_str:",
      "        return None, None",
      "    parts = re.findall(r'[\\d.]+', dms_str)",
      "    dirs = re.findall(r'[NSEW]', dms_str)",
      "    if len(parts) < 6 or len(dirs) < 2:",
      "        return None, None",
      "    lat = float(parts[0]) + float(parts[1]) / 60.0 + float(parts[2]) / 3600.0",
      "    lon = float(parts[3]) + float(parts[4]) / 60.0 + float(parts[5]) / 3600.0",
      "    if dirs[0] == 'S':",
      "        lat = -lat",
      "    if dirs[1] == 'W':",
      "        lon = -lon",
      "    return lat, lon",
      "plot_info = {}",
      "for row in rows_meta[1:]:",
      "    grad = row[0]",
      "    elev = row[1]",
      "    center = row[8]",
      "    date = row[9]",
      "    lat, lon = dms_to_dd(str(center) if center else '')",
      "    if hasattr(date, 'date'):",
      "        date_str = str(date.date())",
      "    elif date is None:",
      "        date_str = ''",
      "    else:",
      "        date_str = str(date)",
      "    if grad and elev is not None:",
      "        try:",
      "            plot_info[(str(grad), int(float(elev)))] = (lat, lon, date_str)",
      "        except Exception:",
      "            pass",
      "results = []",
      "for sheet_name, grad_id in [('La_Parva_gradient', 'La Parva'), ('Valle_Nevado_gradient', 'Valle Nevado')]:",
      "    ws = wb[sheet_name]",
      "    all_rows = list(ws.iter_rows(values_only=True))",
      "    if not all_rows:",
      "        continue",
      "    header = all_rows[0]",
      "    elev_bands = header[1:]",
      "    for data_row in all_rows[1:]:",
      "        species = data_row[0]",
      "        if not species:",
      "            continue",
      "        for i, elev in enumerate(elev_bands):",
      "            if elev is None:",
      "                continue",
      "            val = data_row[i + 1] if i + 1 < len(data_row) else None",
      "            try:",
      "                present = (val is not None and float(val) > 0)",
      "            except Exception:",
      "                present = False",
      "            if not present:",
      "                continue",
      "            try:",
      "                elev_int = int(float(elev))",
      "            except Exception:",
      "                continue",
      "            lat, lon, date_str = plot_info.get((grad_id, elev_int), (None, None, ''))",
      "            results.append([str(species), grad_id, elev_int, lat, lon, date_str])",
      "with open(out_path, 'w', newline='', encoding='utf-8') as f:",
      "    w = csv.writer(f)",
      "    w.writerow(['species', 'gradient_id', 'elevation_m', 'lat', 'lon', 'sampling_date'])",
      "    w.writerows(results)",
      "print('Wrote', len(results), 'rows')"
    )
    writeLines(py_code, py_script)
    py_out <- system2("python3", c(py_script, xlsx_path, csv_path), stdout = TRUE, stderr = TRUE)
    if (!file.exists(csv_path)) {
      stop("Python conversion failed; no output CSV written. Output: ", paste(py_out, collapse = " | "))
    }

    raw_dt <- fread(csv_path, showProgress = FALSE)
    if (nrow(raw_dt) == 0) {
      stop("Converted Arroyo CSV had 0 rows")
    }
    raw_dt[, `:=`(
      species = as.character(species),
      scientificName = as.character(species),
      decimalLatitude = as.character(lat),
      decimalLongitude = as.character(lon),
      eventDate = as.character(sampling_date),
      locality = paste0(as.character(gradient_id), ", ", as.character(elevation_m), "m")
    )]
    raw_dt[, occurrenceID := paste0(s1_id, ":", .I)]

    dt_out <- data.table(
      source_id                     = s1_id,
      occurrenceID                  = raw_dt$occurrenceID,
      species                       = raw_dt$species,
      scientificName                = raw_dt$scientificName,
      taxonRank                     = NA_character_,
      decimalLatitude               = raw_dt$decimalLatitude,
      decimalLongitude              = raw_dt$decimalLongitude,
      coordinateUncertaintyInMeters = NA_character_,
      countryCode                   = "CL",
      country                       = "Chile",
      stateProvince                 = NA_character_,
      locality                      = raw_dt$locality,
      eventDate                     = raw_dt$eventDate,
      year                          = NA_character_,
      month                         = NA_character_,
      day                           = NA_character_,
      basisOfRecord                 = "HumanObservation",
      institutionCode               = NA_character_,
      collectionCode                = NA_character_,
      catalogNumber                 = NA_character_,
      recordedBy                    = NA_character_,
      identifiedBy                  = NA_character_,
      datasetName                   = "Arroyo et al. DIB High Andes Chile",
      gbif_datasetKey               = NA_character_,
      source_doi                    = s1_doi,
      download_timestamp_utc        = TIMESTAMP,
      qa_flags                      = NA_character_
    )

    dt_out <- ensure_dwc_cols(dt_out, s1_id, s1_doi, TIMESTAMP, ds_key = NA_character_)
    safe_write_compiled(dt_out, s1_out)
    list(nrow = nrow(dt_out), status = "compiled")
  }
}, error = function(e) {
  cat("  [ERROR]", conditionMessage(e), "\n")
  list(nrow = 0L, status = paste0("error: ", conditionMessage(e)))
})

# ===========================================================================
# Source 2: manual_central_african_plot_network_cafriplot
# URL: https://cafriplot.net/
# Strategy: fetch HTML, grep for download links (csv, zip, xlsx, zenodo, dryad, doi)
# ===========================================================================
cat("\n=== Source 2: manual_central_african_plot_network_cafriplot ===\n")
s2_id  <- "manual_central_african_plot_network_cafriplot"
s2_doi <- NA_character_
s2_dir <- file.path(out_root, s2_id)
s2_out <- file.path(s2_dir, "compiled_occurrences.csv")

results[[s2_id]] <- tryCatch({
  if (file.exists(s2_out)) {
    cat("  [SKIP] Already compiled:", s2_out, "\n")
    dt <- fread(s2_out, showProgress = FALSE)
    list(nrow = nrow(dt), status = "skipped")
  } else {
    dir.create(s2_dir, recursive = TRUE, showWarnings = FALSE)
    dt_out <- NULL

    # Fetch CAFRIPLOT homepage and look for download links
    cat("  Fetching https://cafriplot.net/ ...\n")
    html_resp <- safe_get("https://cafriplot.net/")
    if (!is.null(html_resp)) {
      html_text <- rawToChar(html_resp$content)
      # Grep for download-indicative patterns
      dl_patterns <- c(
        "href=\"[^\"]*\\.(csv|zip|xlsx|tsv)[^\"]*\"",
        "zenodo\\.org",
        "datadryad\\.org",
        "figshare\\.com",
        "doi\\.org/10\\."
      )
      found_links <- character(0)
      for (pat in dl_patterns) {
        m <- regmatches(html_text, gregexpr(pat, html_text, ignore.case = TRUE, perl = TRUE))[[1]]
        found_links <- c(found_links, m)
      }
      found_links <- unique(found_links)
      cat("  Candidate download links found:", length(found_links), "\n")
      for (lnk in found_links) {
        cat("   ", lnk, "\n")
      }

      # Try to follow any zenodo/dryad/doi links
      doi_links <- grep("doi\\.org/10\\.", found_links, value = TRUE, ignore.case = TRUE)
      for (dl in doi_links) {
        extracted_doi <- regmatches(dl, regexpr("10\\.[0-9]+/[^ \"'>]+", dl))
        if (length(extracted_doi) == 0) next
        cat("  Trying DOI from page:", extracted_doi, "\n")
        ds_key <- gbif_dataset_key(extracted_doi)
        if (!is.na(ds_key)) {
          cat("  Resolves to GBIF dataset key:", ds_key, "\n")
          dt_out <- ingest_from_gbif_key(s2_id, extracted_doi, ds_key)
          if (!is.null(dt_out) && nrow(dt_out) > 0) {
            dt_out[, gbif_datasetKey := ds_key]
            break
          }
        }
      }

      # Try any direct CSV/ZIP hrefs
      if (is.null(dt_out)) {
        href_matches <- regmatches(html_text,
          gregexpr("href=\"([^\"]*\\.(csv|zip|xlsx|tsv)[^\"]*)\"",
                   html_text, ignore.case = TRUE, perl = TRUE))[[1]]
        for (href in href_matches) {
          file_url <- regmatches(href, regexpr("\"[^\"]+\"", href))
          file_url <- gsub("\"", "", file_url)
          if (!grepl("^http", file_url)) {
            file_url <- paste0("https://cafriplot.net", if (!startsWith(file_url, "/")) "/" else "", file_url)
          }
          fname <- basename(file_url)
          raw_dt <- tryCatch(
            download_and_read(file_url, file.path(s2_dir, "raw", fname)),
            error = function(e) { cat("  [WARN]", conditionMessage(e), "\n"); NULL }
          )
          if (!is.null(raw_dt) && nrow(raw_dt) > 0) {
            dt_out <- map_to_dwc(raw_dt, s2_id, s2_doi,
                                 "CAFRIPLOT Central African Plot Network",
                                 bor = "HumanObservation")
            break
          }
        }
      }
    }

    if (is.null(dt_out) || nrow(dt_out) == 0) {
      cat("  [WARN] No downloadable data found — writing 0-row placeholder\n")
      dt_out <- empty_dwc()
      writeLines(
        c("qa_flags: needs_manual_access|contact_required",
          "url: https://cafriplot.net/",
          paste("checked:", TIMESTAMP),
          "note: No CSV/ZIP/XLSX download link found on CAFRIPLOT homepage. Manual contact required."),
        file.path(s2_dir, "needs_manual_access.txt")
      )
      safe_write_compiled(dt_out, s2_out)
      list(nrow = 0L, status = "needs_manual_access|contact_required")
    } else {
      safe_write_compiled(dt_out, s2_out)
      list(nrow = nrow(dt_out), status = "compiled")
    }
  }
}, error = function(e) {
  cat("  [ERROR]", conditionMessage(e), "\n")
  list(nrow = 0L, status = paste0("error: ", conditionMessage(e)))
})

# ===========================================================================
# Source 3: manual_herbase_amazon_herbs
# URL: https://www.scielo.br/j/aa/a/9Tp47pFS4bMsjRT6LkPjcHQ/?lang=en
# Strategy: verified no trusted auto-download source; write explicit placeholder
# ===========================================================================
cat("\n=== Source 3: manual_herbase_amazon_herbs ===\n")
s3_id  <- "manual_herbase_amazon_herbs"
s3_doi <- NA_character_
s3_url <- "https://www.scielo.br/j/aa/a/9Tp47pFS4bMsjRT6LkPjcHQ/?lang=en"
s3_dir <- file.path(out_root, s3_id)
s3_out <- file.path(s3_dir, "compiled_occurrences.csv")

results[[s3_id]] <- tryCatch({
  skip_result <- NULL
  if (file.exists(s3_out)) {
    dt <- fread(s3_out, showProgress = FALSE)
    if (nrow(dt) > 0) {
      cat("  [SKIP] Already compiled with non-empty output:", s3_out, "\n")
      skip_result <- list(nrow = nrow(dt), status = "skipped")
    } else {
      cat("  Existing placeholder is empty; refreshing placeholder metadata\n")
      file.remove(s3_out)
    }
  }
  if (!is.null(skip_result)) {
    skip_result
  } else {
    dir.create(s3_dir, recursive = TRUE, showWarnings = FALSE)
    dt_out <- empty_dwc()
    writeLines(
      c(
        "qa_flags: needs_manual_access|no_verified_download_found",
        paste("url:", s3_url),
        paste("checked:", TIMESTAMP),
        "note: No verified auto-downloadable HERBase dataset source was found; manual access is required."
      ),
      file.path(s3_dir, "needs_manual_access.txt")
    )
    safe_write_compiled(dt_out, s3_out)
    list(nrow = 0L, status = "needs_manual_access|no_verified_download_found")
  }
}, error = function(e) {
  cat("  [ERROR]", conditionMessage(e), "\n")
  list(nrow = 0L, status = paste0("error: ", conditionMessage(e)))
})

# ===========================================================================
# Source 4: manual_red_argentina_parcelas_permanentes
# No URL/DOI — needs discovery
# Strategy: Zenodo search → Dryad search → placeholder
# ===========================================================================
cat("\n=== Source 4: manual_red_argentina_parcelas_permanentes ===\n")
s4_id  <- "manual_red_argentina_parcelas_permanentes"
s4_doi <- NA_character_
s4_dir <- file.path(out_root, s4_id)
s4_out <- file.path(s4_dir, "compiled_occurrences.csv")

results[[s4_id]] <- tryCatch({
  if (file.exists(s4_out)) {
    cat("  [SKIP] Already compiled:", s4_out, "\n")
    dt <- fread(s4_out, showProgress = FALSE)
    list(nrow = nrow(dt), status = "skipped")
  } else {
    dir.create(s4_dir, recursive = TRUE, showWarnings = FALSE)
    dt_out <- NULL
    found_doi <- NA_character_

    # Step 1: Zenodo keyword search
    cat("  Trying Zenodo search...\n")
    z <- zenodo_search_first_csv_url("red argentina parcelas permanentes")
    if (!is.na(z$url)) {
      fname <- if (!is.null(z$fname)) z$fname else paste0(s4_id, "_zenodo.csv")
      raw_dt <- tryCatch(
        download_and_read(z$url, file.path(s4_dir, "raw", fname)),
        error = function(e) { cat("  [WARN]", conditionMessage(e), "\n"); NULL }
      )
      if (!is.null(raw_dt) && nrow(raw_dt) > 0) {
        found_doi <- z$record_doi
        dt_out <- map_to_dwc(raw_dt, s4_id, found_doi,
                             "Red Argentina de Parcelas Permanentes",
                             bor = "HumanObservation")
      }
    }

    # Step 2: Dryad keyword search
    if (is.null(dt_out)) {
      cat("  Trying Dryad search...\n")
      dryad_doi <- dryad_search_first_result("red argentina parcelas permanentes")
      if (!is.na(dryad_doi)) {
        found_doi <- dryad_doi
        cat("  Attempting Dryad ingest for DOI:", dryad_doi, "\n")
        # Fetch Dryad versions
        encoded_doi <- URLencode(paste0("doi:", dryad_doi), reserved = TRUE)
        versions_url <- paste0("https://datadryad.org/api/v2/datasets/", encoded_doi, "/versions")
        resp_ver <- safe_get(versions_url)
        if (!is.null(resp_ver)) {
          ver_parsed <- tryCatch(fromJSON(rawToChar(resp_ver$content), simplifyVector = FALSE),
                                 error = function(e) NULL)
          if (!is.null(ver_parsed)) {
            version_list <- ver_parsed[["_embedded"]][["stash:versions"]]
            if (!is.null(version_list) && length(version_list) > 0) {
              latest_ver <- version_list[[length(version_list)]]
              ver_id <- latest_ver[["id"]]
              if (is.null(ver_id)) {
                self_link <- latest_ver[["_links"]][["self"]][["href"]]
                ver_id <- basename(self_link)
              }
              zip_path <- file.path(s4_dir, "raw", paste0(s4_id, "_dryad.zip"))
              dl_url   <- paste0("https://datadryad.org/api/v2/versions/", ver_id, "/download")
              raw_dt <- tryCatch(
                download_and_read(dl_url, zip_path),
                error = function(e) { cat("  [WARN]", conditionMessage(e), "\n"); NULL }
              )
              if (!is.null(raw_dt) && nrow(raw_dt) > 0) {
                dt_out <- map_to_dwc(raw_dt, s4_id, dryad_doi,
                                     "Red Argentina de Parcelas Permanentes",
                                     bor = "HumanObservation")
              }
            }
          }
        }
      }
    }

    if (is.null(dt_out) || nrow(dt_out) == 0) {
      cat("  [WARN] No dataset found — writing 0-row placeholder\n")
      dt_out <- empty_dwc()
      writeLines(
        c("qa_flags: needs_manual_access|contact_required",
          "note: No URL/DOI known. Zenodo and Dryad searches returned no matching dataset.",
          paste("checked:", TIMESTAMP)),
        file.path(s4_dir, "needs_manual_access.txt")
      )
      safe_write_compiled(dt_out, s4_out)
      list(nrow = 0L, status = "needs_manual_access|contact_required")
    } else {
      safe_write_compiled(dt_out, s4_out)
      list(nrow = nrow(dt_out), status = "compiled")
    }
  }
}, error = function(e) {
  cat("  [ERROR]", conditionMessage(e), "\n")
  list(nrow = 0L, status = paste0("error: ", conditionMessage(e)))
})

# ===========================================================================
# Source 5: manual_russian_arctic_vegetation_archive
# DOI: 10.5061/dryad.5tb2rbp8d
# Strategy: parse extracted Dryad CSV pairs (*_species_data.csv + *_habitat_data.csv)
# ===========================================================================
cat("\n=== Source 5: manual_russian_arctic_vegetation_archive ===\n")
s5_id    <- "manual_russian_arctic_vegetation_archive"
s5_doi   <- "10.5061/dryad.5tb2rbp8d"
s5_dir   <- file.path(out_root, s5_id)
s5_out   <- file.path(s5_dir, "compiled_occurrences.csv")

results[[s5_id]] <- tryCatch({
  skip_result <- NULL
  if (file.exists(s5_out)) {
    dt <- fread(s5_out, showProgress = FALSE)
    if (nrow(dt) > 0) {
      cat("  [SKIP] Already compiled with non-empty output:", s5_out, "\n")
      skip_result <- list(nrow = nrow(dt), status = "skipped")
    } else {
      cat("  Existing output is empty; rebuilding from extracted Dryad files\n")
      file.remove(s5_out)
    }
  }
  if (!is.null(skip_result)) {
    skip_result
  } else {
    dir.create(s5_dir, recursive = TRUE, showWarnings = FALSE)
    extracted_dir <- file.path(s5_dir, "raw", paste0(s5_id, "_dryad.zip_extracted"))
    raw_dir <- file.path(s5_dir, "raw")
    dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
    if (!dir.exists(extracted_dir)) {
      cat("  Extracted Dryad folder not found; downloading and extracting DOI dataset...\n")
      encoded_doi <- URLencode(paste0("doi:", s5_doi), reserved = TRUE)
      versions_url <- paste0("https://datadryad.org/api/v2/datasets/", encoded_doi, "/versions")
      resp_ver <- safe_get(versions_url)
      if (is.null(resp_ver)) {
        stop("Unable to retrieve Dryad versions API for ", s5_doi)
      }
      ver_parsed <- tryCatch(fromJSON(rawToChar(resp_ver$content), simplifyVector = FALSE),
                             error = function(e) NULL)
      if (is.null(ver_parsed) || is.null(ver_parsed[["_embedded"]][["stash:versions"]])) {
        stop("Dryad versions payload missing for ", s5_doi)
      }
      version_list <- ver_parsed[["_embedded"]][["stash:versions"]]
      if (!length(version_list)) {
        stop("Dryad versions API returned no versions for ", s5_doi)
      }
      latest_ver <- version_list[[length(version_list)]]
      ver_id <- latest_ver[["id"]]
      if (is.null(ver_id)) {
        ver_id <- basename(latest_ver[["_links"]][["self"]][["href"]])
      }
      zip_path <- file.path(raw_dir, paste0(s5_id, "_dryad.zip"))
      dl_url <- paste0("https://datadryad.org/api/v2/versions/", ver_id, "/download")
      resp_zip <- GET(dl_url, write_disk(zip_path, overwrite = TRUE), timeout(120))
      if (status_code(resp_zip) >= 400) {
        stop("Dryad ZIP download failed with HTTP ", status_code(resp_zip))
      }
      unzip(zip_path, exdir = extracted_dir)
      if (!dir.exists(extracted_dir)) {
        stop("Failed to extract Dryad ZIP into ", extracted_dir)
      }
    }

    species_files <- list.files(extracted_dir, pattern = "_species_data\\.csv$", recursive = TRUE, full.names = TRUE)
    if (!length(species_files)) {
      stop("No *_species_data.csv files found in extracted Dryad directory")
    }
    cat("  Found species files:", length(species_files), "\n")

    clean_chr <- function(x) {
      trimws(iconv(as.character(x), from = "", to = "UTF-8", sub = ""))
    }

    parse_habitat_file <- function(path) {
      raw <- fread(path, header = FALSE, fill = TRUE, sep = ",", quote = "\"", encoding = "Latin-1", showProgress = FALSE)
      idx <- which(clean_chr(raw[[1]]) == "RELEVE_NR")
      if (!length(idx)) return(NULL)
      hdr <- clean_chr(unlist(raw[idx[1], ], use.names = FALSE))
      hdr[is.na(hdr) | hdr == ""] <- paste0("V", which(is.na(hdr) | hdr == ""))
      dat <- raw[(idx[1] + 1):nrow(raw)]
      if (!nrow(dat)) return(NULL)
      setnames(dat, hdr)
      dat <- dat[clean_chr(RELEVE_NR) != ""]
      dat
    }

    parse_species_file <- function(path) {
      raw <- fread(path, header = FALSE, fill = TRUE, sep = ",", quote = "\"", encoding = "Latin-1", showProgress = FALSE)
      if (nrow(raw) == 0) return(NULL)

      turb_idx <- which(clean_chr(raw[[3]]) == "TURBOVEG PLOT NUMBER")
      head_idx <- which(clean_chr(raw[[1]]) == "PASL TAXON SCIENTIFIC NAME NO AUTHOR(S)")
      if (!length(turb_idx) || !length(head_idx)) return(NULL)
      turb_idx <- turb_idx[1]
      head_idx <- head_idx[head_idx >= turb_idx][1]
      if (is.na(head_idx)) return(NULL)

      plot_ids <- clean_chr(unlist(raw[turb_idx, 4:ncol(raw)], use.names = FALSE))
      keep_cols <- which(nzchar(plot_ids))
      if (!length(keep_cols)) return(NULL)

      dat <- raw[(head_idx + 1):nrow(raw)]
      if (!nrow(dat)) return(NULL)
      needed_cols <- c(1, 2, 3, keep_cols + 3)
      needed_cols <- needed_cols[needed_cols <= ncol(dat)]
      dat <- dat[, ..needed_cols]
      col_names <- c("species", "scientificName", "dataset_taxon", plot_ids[keep_cols])
      col_names <- col_names[seq_len(ncol(dat))]
      setnames(dat, col_names)

      dat[, species := clean_chr(species)]
      dat[, scientificName := clean_chr(scientificName)]
      dat <- dat[species != "" & !is.na(species)]
      if (!nrow(dat) || ncol(dat) <= 3) return(NULL)

      long <- melt(
        dat,
        id.vars = c("species", "scientificName", "dataset_taxon"),
        variable.name = "RELEVE_NR",
        value.name = "cover_value",
        variable.factor = FALSE
      )
      long[, cover_value := clean_chr(cover_value)]
      long <- long[
        cover_value != "" & !is.na(cover_value) &
          !cover_value %in% c("0", "0.0", "0.00")
      ]
      long
    }

    map_country <- function(x) {
      x_chr <- clean_chr(x)
      x_up <- toupper(x_chr)
      mapped <- fcase(
        x_up == "RU", "Russia",
        x_up == "NO", "Norway",
        x_up == "SE", "Sweden",
        x_up == "FI", "Finland",
        x_up == "IS", "Iceland",
        x_up == "US", "United States",
        x_up == "CA", "Canada",
        default = x_chr
      )
      mapped
    }

    all_rows <- list()
    for (sp in species_files) {
      hab <- sub("_species_data\\.csv$", "_habitat_data.csv", sp)
      if (!file.exists(hab)) {
        cat("  [WARN] Missing habitat pair for:", basename(sp), "\n")
        next
      }
      cat("  Parsing pair:", basename(sp), "\n")
      sp_long <- parse_species_file(sp)
      hab_dt <- parse_habitat_file(hab)
      if (is.null(sp_long) || !nrow(sp_long) || is.null(hab_dt) || !nrow(hab_dt)) {
        cat("  [WARN] Empty parsed pair for:", basename(sp), "\n")
        next
      }

      hab_dt[, RELEVE_NR := clean_chr(RELEVE_NR)]
      sp_long[, RELEVE_NR := clean_chr(RELEVE_NR)]

      keep_hab <- intersect(c("RELEVE_NR", "LATITUDE", "LONGITUDE", "DATE", "REGION", "LOCATION", "COUNTRY"), names(hab_dt))
      if (!all(c("RELEVE_NR", "LATITUDE", "LONGITUDE") %in% keep_hab)) {
        cat("  [WARN] Missing key habitat columns in:", basename(hab), "\n")
        next
      }
      hab_use <- unique(hab_dt[, ..keep_hab])

      joined <- merge(sp_long, hab_use, by = "RELEVE_NR", all.x = TRUE)
      if (!nrow(joined)) next
      joined[, source_file := clean_chr(basename(sp))]
      all_rows[[length(all_rows) + 1L]] <- joined
    }

    if (!length(all_rows)) {
      stop("No usable species/habitat pairs produced rows")
    }

    dt_raw <- rbindlist(all_rows, fill = TRUE, use.names = TRUE)
    dt_raw[, locality := fifelse(
      !is.na(REGION) & clean_chr(REGION) != "" &
        !is.na(LOCATION) & clean_chr(LOCATION) != "",
      paste0(clean_chr(REGION), ": ", clean_chr(LOCATION)),
      clean_chr(REGION)
    )]

    dt_out <- data.table(
      source_id                     = s5_id,
      occurrenceID                  = paste0(s5_id, ":", dt_raw$source_file, ":", dt_raw$RELEVE_NR, ":", seq_len(nrow(dt_raw))),
      species                       = as.character(dt_raw$species),
      scientificName                = fifelse(
        !is.na(dt_raw$scientificName) & clean_chr(dt_raw$scientificName) != "",
        clean_chr(dt_raw$scientificName),
        clean_chr(dt_raw$species)
      ),
      taxonRank                     = NA_character_,
      decimalLatitude               = clean_chr(dt_raw$LATITUDE),
      decimalLongitude              = clean_chr(dt_raw$LONGITUDE),
      coordinateUncertaintyInMeters = NA_character_,
      countryCode                   = clean_chr(dt_raw$COUNTRY),
      country                       = map_country(dt_raw$COUNTRY),
      stateProvince                 = clean_chr(dt_raw$REGION),
      locality                      = dt_raw$locality,
      eventDate                     = clean_chr(dt_raw$DATE),
      year                          = NA_character_,
      month                         = NA_character_,
      day                           = NA_character_,
      basisOfRecord                 = "HumanObservation",
      institutionCode               = NA_character_,
      collectionCode                = NA_character_,
      catalogNumber                 = NA_character_,
      recordedBy                    = NA_character_,
      identifiedBy                  = NA_character_,
      datasetName                   = "Russian Arctic Vegetation Archive",
      gbif_datasetKey               = NA_character_,
      source_doi                    = s5_doi,
      download_timestamp_utc        = TIMESTAMP,
      qa_flags                      = "braun_blanquet_cover_retained"
    )

    dt_out <- ensure_dwc_cols(dt_out, s5_id, s5_doi, TIMESTAMP, ds_key = NA_character_)
    # Flag coordinates that are out of plausible range
    lat_num <- suppressWarnings(as.numeric(dt_out$decimalLatitude))
    lon_num <- suppressWarnings(as.numeric(dt_out$decimalLongitude))
    bad_coords <- !is.na(lat_num) & (lat_num < -90 | lat_num > 90 |
                                      lon_num < -180 | lon_num > 180)
    if (any(bad_coords, na.rm = TRUE)) {
      dt_out[bad_coords, qa_flags := paste0(qa_flags, "|out_of_range_coordinates")]
      cat("  [WARN] Flagged", sum(bad_coords, na.rm = TRUE), "rows with out-of-range coordinates\n")
    }
    safe_write_compiled(dt_out, s5_out)
    list(nrow = nrow(dt_out), status = "compiled")
  }
}, error = function(e) {
  cat("  [ERROR]", conditionMessage(e), "\n")
  list(nrow = 0L, status = paste0("error: ", conditionMessage(e)))
})

# ===========================================================================
# Summary
# ===========================================================================
cat("\n=== Summary by source ===\n")
for (sid in names(results)) {
  r <- results[[sid]]
  cat(sprintf("  %-55s  %6d rows  [%s]\n", sid, r$nrow, r$status))
}

cat("\nDone.\n")
