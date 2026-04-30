#!/usr/bin/env Rscript
# Download 3 GBIF occurrence datasets that failed in the initial run
# because the DOI->key lookup used the wrong API endpoint.
# Resolved UUIDs obtained via doi.org redirect.

library(httr)
library(jsonlite)
library(data.table)

FINAL_COLS <- c(
  "source_id","occurrenceID","species","scientificName","taxonRank",
  "decimalLatitude","decimalLongitude","coordinateUncertaintyInMeters",
  "countryCode","country","stateProvince","locality","eventDate",
  "year","month","day","basisOfRecord","institutionCode","collectionCode",
  "catalogNumber","recordedBy","identifiedBy","datasetName",
  "gbif_datasetKey","source_doi","download_timestamp_utc","qa_flags"
)

fetch_gbif <- function(src_id, doi, dataset_key, max_recs = 10000L) {
  out_dir  <- file.path("data/occurrences", src_id)
  out_file <- file.path(out_dir, "compiled_occurrences.csv")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  if (file.exists(out_file)) {
    cat(src_id, "-> already compiled, skipping\n")
    return(invisible(NULL))
  }

  ts   <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  rows <- list()
  offset <- 0L
  limit  <- 300L
  get_col <- function(df, col) {
    if (col %in% names(df)) as.character(df[[col]]) else NA_character_
  }

  repeat {
    url  <- sprintf(
      "https://api.gbif.org/v1/occurrence/search?datasetKey=%s&limit=%d&offset=%d",
      dataset_key, limit, offset)
    resp <- tryCatch(GET(url, timeout(30)), error = function(e) NULL)
    if (is.null(resp) || status_code(resp) != 200) {
      cat(sprintf("  %s: HTTP %s at offset %d — stopping\n",
                  src_id, if (is.null(resp)) "ERROR" else status_code(resp), offset))
      break
    }
    dat <- fromJSON(rawToChar(resp$content))
    res <- dat$results
    if (!length(res) || nrow(res) == 0L) break

    rows[[length(rows) + 1L]] <- data.table(
      source_id = src_id,
      occurrenceID = get_col(res, "key"),
      species = get_col(res, "species"),
      scientificName = get_col(res, "scientificName"),
      taxonRank = get_col(res, "taxonRank"),
      decimalLatitude = get_col(res, "decimalLatitude"),
      decimalLongitude = get_col(res, "decimalLongitude"),
      coordinateUncertaintyInMeters = get_col(res, "coordinateUncertaintyInMeters"),
      countryCode = get_col(res, "countryCode"),
      country = get_col(res, "country"),
      stateProvince = get_col(res, "stateProvince"),
      locality = get_col(res, "locality"),
      eventDate = get_col(res, "eventDate"),
      year = get_col(res, "year"),
      month = get_col(res, "month"),
      day = get_col(res, "day"),
      basisOfRecord = get_col(res, "basisOfRecord"),
      institutionCode = get_col(res, "institutionCode"),
      collectionCode = get_col(res, "collectionCode"),
      catalogNumber = get_col(res, "catalogNumber"),
      recordedBy = get_col(res, "recordedBy"),
      identifiedBy = get_col(res, "identifiedBy"),
      datasetName = get_col(res, "datasetName"),
      gbif_datasetKey = dataset_key,
      source_doi = doi,
      download_timestamp_utc = ts,
      qa_flags = NA_character_
    )
    cat(sprintf("  %s: offset %d -> %d rows\n", src_id, offset, nrow(res)))
    offset <- offset + limit
    if (isTRUE(dat$endOfRecords) || offset >= max_recs) break
    Sys.sleep(0.5)
  }

  if (!length(rows)) {
    cat(src_id, "-> 0 rows compiled\n")
    return(invisible(NULL))
  }

  out <- rbindlist(rows, fill = TRUE, use.names = TRUE)
  for (col in setdiff(FINAL_COLS, names(out))) out[[col]] <- NA_character_
  setcolorder(out, intersect(FINAL_COLS, names(out)))
  fwrite(out, out_file)
  cat(src_id, "-> wrote", nrow(out), "rows to", out_file, "\n")
}

fetch_gbif("manual_flora_sumatra_gbif_anda2",
           "10.15468/55evew",
           "39e85504-1ebe-4671-be65-19ccdc1d7c7d")

fetch_gbif("manual_flora_sumatra_batang_toru",
           "10.15468/ue7xyn",
           "10f8ba9a-e298-4256-88b0-997205d66a30")

fetch_gbif("manual_pucv_herbarium_gbif",
           "10.15468/k485f5",
           "0f99f0f9-e32a-4deb-ad51-4e729dc9f274")

cat("All done.\n")
