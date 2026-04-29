#!/usr/bin/env Rscript

args_all <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_path <- sub(file_arg, "", args_all[grep(file_arg, args_all)][1])
project_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
source(file.path(project_root, "scripts", "utils.R"), local = FALSE)

if (!requireNamespace("readxl", quietly = TRUE)) {
  stop("Package 'readxl' is required for XLSX parsing.", call. = FALSE)
}

combine_event_date <- function(df) {
  if ("eventDate" %in% names(df)) {
    out <- as.character(df$eventDate)
    out[!nzchar(out)] <- NA_character_
    return(out)
  }
  has_dmy <- all(c("day", "month", "year") %in% names(df))
  if (has_dmy) {
    d <- suppressWarnings(as.integer(df$day))
    m <- suppressWarnings(as.integer(df$month))
    y <- suppressWarnings(as.integer(df$year))
    ok <- !is.na(d) & !is.na(m) & !is.na(y)
    out <- rep(NA_character_, nrow(df))
    out[ok] <- sprintf("%04d-%02d-%02d", y[ok], m[ok], d[ok])
    return(out)
  }
  if ("last_coll" %in% names(df)) {
    y <- suppressWarnings(as.integer(df$last_coll))
    out <- rep(NA_character_, nrow(df))
    out[!is.na(y)] <- sprintf("%04d", y[!is.na(y)])
    return(out)
  }
  rep(NA_character_, nrow(df))
}

extract_scientific_name <- function(df) {
  if ("species" %in% names(df)) return(as.character(df$species))
  if ("scientificName" %in% names(df)) return(as.character(df$scientificName))
  rep(NA_character_, nrow(df))
}

args <- parse_named_args(commandArgs(trailingOnly = TRUE))
root <- find_project_root()
paper_id <- args$`paper-id` %||% "jennings_2026"
raw_dir <- file.path(root, "data", "raw", paper_id)
interim_dir <- args$`interim-dir` %||% file.path(root, "data", "interim")
map_path <- args$`mapping-file` %||% file.path(root, "mappings", "jennings_2026_column_mapping.csv")
log_file <- file.path(root, "logs", paste0("normalize_", paper_id, ".log"))

if (!dir.exists(raw_dir)) stop("Missing raw directory: ", raw_dir, call. = FALSE)
if (!file.exists(map_path)) stop("Missing mapping file: ", map_path, call. = FALSE)

papers <- read_papers_config(root)
paper <- papers[papers$paper_id == paper_id, , drop = FALSE]
if (!nrow(paper)) stop("paper_id not found in config: ", paper_id, call. = FALSE)

mapping <- utils::read.csv(map_path, stringsAsFactors = FALSE)
mapping$source_column_norm <- normalize_name(mapping$source_column)

files <- list.files(raw_dir, full.names = TRUE)
if (!length(files)) stop("No downloaded files in: ", raw_dir, call. = FALSE)

tabular_files <- files[grepl("\\.(xlsx|xls|csv|tsv)$", files, ignore.case = TRUE)]
if (!length(tabular_files)) {
  append_log(log_file, "WARN", "No tabular files found", raw_dir)
  stop("No tabular files to normalize.", call. = FALSE)
}

rows_out <- list()
idx <- 0L

for (f in tabular_files) {
  ext <- tolower(tools::file_ext(f))
  if (ext %in% c("xlsx", "xls")) {
    sheet_names <- tryCatch(readxl::excel_sheets(f), error = function(e) character(0))
    if (!length(sheet_names)) {
      append_log(log_file, "WARN", "Unreadable workbook", f)
      next
    }
    for (sheet in sheet_names) {
      df <- tryCatch(as.data.frame(readxl::read_excel(f, sheet = sheet)), error = function(e) NULL)
      if (is.null(df) || !nrow(df)) next
      names(df) <- normalize_name(names(df))

      scientific_name <- extract_scientific_name(df)
      event_date <- combine_event_date(df)
      coord_unc_km <- if ("coordinateuncertaintyinkilometers" %in% names(df)) safe_numeric(df$coordinateuncertaintyinkilometers) else rep(NA_real_, nrow(df))

      idx <- idx + 1L
      rows_out[[idx]] <- data.frame(
        paper_id = paper_id,
        doi = paper$doi[[1]],
        citation = paper$citation[[1]],
        source_url = paper$landing_url[[1]],
        source_file = basename(f),
        source_sheet = sheet,
        original_row_number = seq_len(nrow(df)),
        scientificName = scientific_name,
        scientificNameAuthorship = if ("authority" %in% names(df)) as.character(df$authority) else NA_character_,
        family = if ("family" %in% names(df)) as.character(df$family) else NA_character_,
        genus = if ("genus" %in% names(df)) as.character(df$genus) else NA_character_,
        basisOfRecord = if ("basisofrecord" %in% names(df)) as.character(df$basisofrecord) else NA_character_,
        recordedBy = if ("collector" %in% names(df)) as.character(df$collector) else NA_character_,
        catalogNumber = if ("catalognumber" %in% names(df)) as.character(df$catalognumber) else NA_character_,
        locality = if ("locality" %in% names(df)) as.character(df$locality) else NA_character_,
        country = if ("country" %in% names(df)) as.character(df$country) else NA_character_,
        island = if ("island" %in% names(df)) as.character(df$island) else NA_character_,
        decimalLatitude = if ("decimallatitude" %in% names(df)) safe_numeric(df$decimallatitude) else NA_real_,
        decimalLongitude = if ("decimallongitude" %in% names(df)) safe_numeric(df$decimallongitude) else NA_real_,
        coordinateUncertaintyInMeters = ifelse(is.na(coord_unc_km), NA_real_, coord_unc_km * 1000),
        minimumElevationInMeters = if ("minimumelevationinmeters" %in% names(df)) safe_numeric(df$minimumelevationinmeters) else NA_real_,
        maximumElevationInMeters = if ("maximumelevationinmeters" %in% names(df)) safe_numeric(df$maximumelevationinmeters) else NA_real_,
        eventDate = event_date,
        occurrenceRemarks = if ("cons_ass" %in% names(df)) as.character(df$cons_ass) else NA_character_,
        stringsAsFactors = FALSE
      )
    }
  } else if (ext %in% c("csv", "tsv")) {
    sep <- if (ext == "tsv") "\t" else ","
    df <- tryCatch(utils::read.csv(f, sep = sep, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
    if (is.null(df) || !nrow(df)) next
    names(df) <- normalize_name(names(df))
    idx <- idx + 1L
    rows_out[[idx]] <- data.frame(
      paper_id = paper_id,
      doi = paper$doi[[1]],
      citation = paper$citation[[1]],
      source_url = paper$landing_url[[1]],
      source_file = basename(f),
      source_sheet = "",
      original_row_number = seq_len(nrow(df)),
      scientificName = extract_scientific_name(df),
      scientificNameAuthorship = NA_character_,
      family = if ("family" %in% names(df)) as.character(df$family) else NA_character_,
      genus = if ("genus" %in% names(df)) as.character(df$genus) else NA_character_,
      basisOfRecord = if ("basisofrecord" %in% names(df)) as.character(df$basisofrecord) else NA_character_,
      recordedBy = if ("collector" %in% names(df)) as.character(df$collector) else NA_character_,
      catalogNumber = if ("catalognumber" %in% names(df)) as.character(df$catalognumber) else NA_character_,
      locality = if ("locality" %in% names(df)) as.character(df$locality) else NA_character_,
      country = if ("country" %in% names(df)) as.character(df$country) else NA_character_,
      island = if ("island" %in% names(df)) as.character(df$island) else NA_character_,
      decimalLatitude = if ("decimallatitude" %in% names(df)) safe_numeric(df$decimallatitude) else NA_real_,
      decimalLongitude = if ("decimallongitude" %in% names(df)) safe_numeric(df$decimallongitude) else NA_real_,
      coordinateUncertaintyInMeters = NA_real_,
      minimumElevationInMeters = NA_real_,
      maximumElevationInMeters = NA_real_,
      eventDate = combine_event_date(df),
      occurrenceRemarks = NA_character_,
      stringsAsFactors = FALSE
    )
  }
}

if (!length(rows_out)) stop("No tabular rows parsed from downloaded files.", call. = FALSE)

norm <- do.call(rbind, rows_out)
norm <- norm[!(is.na(norm$scientificName) | !nzchar(trimws(norm$scientificName))), , drop = FALSE]

out_path <- file.path(interim_dir, paste0(paper_id, "_dwc_normalized.csv"))
write_csv(norm, out_path)
append_log(log_file, "INFO", "Normalization complete", paste("rows=", nrow(norm), " output=", out_path))
message("Normalization complete: ", nrow(norm), " rows -> ", out_path)
