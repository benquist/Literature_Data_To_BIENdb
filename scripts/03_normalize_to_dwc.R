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
  # names(df) are already normalize_name()'d (lowercase) at call time
  date_col <- if ("eventdate" %in% names(df)) "eventdate" else if ("eventDate" %in% names(df)) "eventDate" else NULL
  if (!is.null(date_col)) {
    out <- as.character(df[[date_col]])
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

extract_event_parts <- function(df, event_date) {
  yr <- if ("year" %in% names(df)) suppressWarnings(as.integer(df$year)) else rep(NA_integer_, nrow(df))
  mo <- if ("month" %in% names(df)) suppressWarnings(as.integer(df$month)) else rep(NA_integer_, nrow(df))
  dy <- if ("day" %in% names(df)) suppressWarnings(as.integer(df$day)) else rep(NA_integer_, nrow(df))

  if ("last_coll" %in% names(df)) {
    last_coll_y <- suppressWarnings(as.integer(df$last_coll))
    use_last <- is.na(yr) & !is.na(last_coll_y)
    yr[use_last] <- last_coll_y[use_last]
  }

  parsed_y <- suppressWarnings(as.integer(substr(event_date, 1, 4)))
  parsed_m <- suppressWarnings(as.integer(substr(event_date, 6, 7)))
  parsed_d <- suppressWarnings(as.integer(substr(event_date, 9, 10)))

  yr[is.na(yr) & !is.na(parsed_y)] <- parsed_y[is.na(yr) & !is.na(parsed_y)]
  mo[is.na(mo) & !is.na(parsed_m)] <- parsed_m[is.na(mo) & !is.na(parsed_m)]
  dy[is.na(dy) & !is.na(parsed_d)] <- parsed_d[is.na(dy) & !is.na(parsed_d)]

  list(eventYear = yr, eventMonth = mo, eventDay = dy)
}

extract_scientific_name <- function(df) {
  # names(df) will already be normalize_name()'d (all lowercase) at call time
  if ("species" %in% names(df)) return(as.character(df$species))
  if ("scientificname" %in% names(df)) return(as.character(df$scientificname))
  if ("scientific_name" %in% names(df)) return(as.character(df$scientific_name))
  rep(NA_character_, nrow(df))
}

first_matching_col <- function(df, candidates) {
  nms <- names(df)
  hits <- candidates[candidates %in% nms]
  if (!length(hits)) return(NULL)
  hits[[1]]
}

extract_numeric_candidate <- function(df, candidates) {
  col <- first_matching_col(df, candidates)
  if (is.null(col)) return(rep(NA_real_, nrow(df)))
  safe_numeric(df[[col]])
}

extract_character_candidate <- function(df, candidates) {
  col <- first_matching_col(df, candidates)
  if (is.null(col)) return(rep(NA_character_, nrow(df)))
  out <- as.character(df[[col]])
  out[!nzchar(trimws(out))] <- NA_character_
  out
}

compose_political_units_raw <- function(country, country_code, state_province, county, municipality) {
  n <- length(country)
  out <- rep(NA_character_, n)
  for (i in seq_len(n)) {
    parts <- c(
      if (!is.na(country[[i]]) && nzchar(country[[i]])) paste0("country=", country[[i]]) else NA_character_,
      if (!is.na(country_code[[i]]) && nzchar(country_code[[i]])) paste0("countryCode=", country_code[[i]]) else NA_character_,
      if (!is.na(state_province[[i]]) && nzchar(state_province[[i]])) paste0("stateProvince=", state_province[[i]]) else NA_character_,
      if (!is.na(county[[i]]) && nzchar(county[[i]])) paste0("county=", county[[i]]) else NA_character_,
      if (!is.na(municipality[[i]]) && nzchar(municipality[[i]])) paste0("municipality=", municipality[[i]]) else NA_character_
    )
    parts <- parts[!is.na(parts)]
    if (length(parts)) out[[i]] <- paste(parts, collapse = " | ")
  }
  out
}

extract_coordinate_fields <- function(df) {
  lat_candidates <- c("decimallatitude", "latitude", "lat", "lat_dd", "y")
  lon_candidates <- c("decimallongitude", "longitude", "lon", "long", "lng", "long_dd", "x")

  lat_col <- first_matching_col(df, lat_candidates)
  lon_col <- first_matching_col(df, lon_candidates)

  lat_raw <- if (is.null(lat_col)) rep(NA_character_, nrow(df)) else as.character(df[[lat_col]])
  lon_raw <- if (is.null(lon_col)) rep(NA_character_, nrow(df)) else as.character(df[[lon_col]])
  lat_raw[!nzchar(trimws(lat_raw))] <- NA_character_
  lon_raw[!nzchar(trimws(lon_raw))] <- NA_character_

  lat <- if (is.null(lat_col)) rep(NA_real_, nrow(df)) else safe_numeric(df[[lat_col]])
  lon <- if (is.null(lon_col)) rep(NA_real_, nrow(df)) else safe_numeric(df[[lon_col]])

  invalid_lat <- !is.na(lat) & (lat < -90 | lat > 90)
  invalid_lon <- !is.na(lon) & (lon < -180 | lon > 180)

  lat[invalid_lat] <- NA_real_
  lon[invalid_lon] <- NA_real_

  list(
    decimalLatitude = lat,
    decimalLongitude = lon,
    verbatimLatitude = lat_raw,
    verbatimLongitude = lon_raw,
    n_invalid = sum(invalid_lat | invalid_lon)
  )
}

build_occurrence_id <- function(df, paper_id, source_file, source_sheet, original_row_number) {
  supplied <- extract_character_candidate(df, c("occurrenceid", "occurrence_id", "occid", "id"))
  supplied_ok <- !is.na(supplied) & nzchar(trimws(supplied))
  generated <- paste0(
    paper_id,
    "::",
    source_file,
    "::",
    ifelse(nzchar(source_sheet), source_sheet, "sheet0"),
    "::",
    original_row_number
  )
  supplied[supplied_ok] <- trimws(supplied[supplied_ok])
  supplied[!supplied_ok] <- generated[!supplied_ok]
  supplied
}

build_normalized_block <- function(df, paper, paper_id, source_file, source_sheet) {
  names(df) <- normalize_name(names(df))
  n <- nrow(df)
  original_row_number <- seq_len(n)

  scientific_name <- extract_scientific_name(df)
  event_date <- combine_event_date(df)
  event_parts <- extract_event_parts(df, event_date)
  coord_unc_km <- if ("coordinateuncertaintyinkilometers" %in% names(df)) safe_numeric(df$coordinateuncertaintyinkilometers) else rep(NA_real_, n)
  coords <- extract_coordinate_fields(df)
  elev_fields <- extract_elevation_fields(df)

  catalog_number <- extract_character_candidate(df, c("catalognumber", "catalog_number"))
  record_number <- extract_character_candidate(df, c("recordnumber", "record_number", "fieldnumber", "field_number", "vouchernumber"))
  locality <- extract_character_candidate(df, c("locality", "verbatimlocality"))
  verbatim_locality <- extract_character_candidate(df, c("verbatimlocality", "locality"))
  country <- extract_character_candidate(df, c("country", "countryname"))
  country_code <- extract_character_candidate(df, c("countrycode", "country_code"))
  state_province <- extract_character_candidate(df, c("stateprovince", "state_province", "province", "state"))
  county <- extract_character_candidate(df, c("county", "county_parish", "district"))
  municipality <- extract_character_candidate(df, c("municipality", "municipio", "city", "town"))
  island <- extract_character_candidate(df, c("island"))
  island_group <- extract_character_candidate(df, c("islandgroup", "island_group", "archipelago"))
  water_body <- extract_character_candidate(df, c("waterbody", "water_body", "sea", "ocean"))
  locality_notes <- extract_character_candidate(df, c("localitynotes", "locality_notes", "notes", "locationremarks"))
  habitat <- extract_character_candidate(df, c("habitat"))
  georef_remarks <- extract_character_candidate(df, c("georeferenceremarks", "georefremarks", "locationaccordingto"))
  geodetic_datum <- extract_character_candidate(df, c("geodeticdatum", "datum"))
  occurrence_status <- extract_character_candidate(df, c("occurrencestatus", "priority_species"))
  establishment_means <- extract_character_candidate(df, c("establishmentmeans", "native_status"))
  habit_raw <- extract_character_candidate(df, c("habit", "growthform", "growth_form", "lifeform", "life_form"))

  basis_of_record <- extract_character_candidate(df, c("basisofrecord"))
  inferred_specimen <- is.na(basis_of_record) & !is.na(catalog_number)
  basis_of_record[inferred_specimen] <- "PreservedSpecimen"
  basis_of_record[is.na(basis_of_record)] <- "HumanObservation"

  occurrence_id <- build_occurrence_id(df, paper_id, source_file, source_sheet, original_row_number)
  political_units_raw <- compose_political_units_raw(country, country_code, state_province, county, municipality)

  data.frame(
    paper_id = paper_id,
    doi = paper$doi[[1]],
    citation = paper$citation[[1]],
    source_url = paper$landing_url[[1]],
    source_file = source_file,
    source_sheet = source_sheet,
    original_row_number = original_row_number,
    occurrenceID = occurrence_id,
    scientificName = scientific_name,
    scientificNameAuthorship = extract_character_candidate(df, c("authority", "scientificnameauthorship")),
    family = extract_character_candidate(df, c("family")),
    genus = extract_character_candidate(df, c("genus")),
    basisOfRecord = basis_of_record,
    occurrenceStatus = occurrence_status,
    establishmentMeans = establishment_means,
    recordedBy = extract_character_candidate(df, c("collector", "recordedby")),
    catalogNumber = catalog_number,
    recordNumber = record_number,
    locality = locality,
    verbatimLocality = verbatim_locality,
    localityNotes = locality_notes,
    country = country,
    countryCode = country_code,
    stateProvince = state_province,
    county = county,
    municipality = municipality,
    island = island,
    islandGroup = island_group,
    waterBody = water_body,
    politicalUnitsRaw = political_units_raw,
    decimalLatitude = coords$decimalLatitude,
    decimalLongitude = coords$decimalLongitude,
    verbatimLatitude = coords$verbatimLatitude,
    verbatimLongitude = coords$verbatimLongitude,
    coordinateUncertaintyInMeters = ifelse(is.na(coord_unc_km), NA_real_, coord_unc_km * 1000),
    geodeticDatum = geodetic_datum,
    georeferenceRemarks = georef_remarks,
    verbatimElevation = elev_fields$verbatimElevation,
    minimumElevationInMeters = elev_fields$minimumElevationInMeters,
    maximumElevationInMeters = elev_fields$maximumElevationInMeters,
    elevation = elev_fields$elevation,
    habitat = habitat,
    habit = habit_raw,
    eventDate = event_date,
    eventYear = event_parts$eventYear,
    eventMonth = event_parts$eventMonth,
    eventDay = event_parts$eventDay,
    occurrenceRemarks = extract_character_candidate(df, c("occurrenceremarks", "cons_ass")),
    sourceProvenance = paste0(source_file, ifelse(nzchar(source_sheet), paste0("#", source_sheet), "")),
    stringsAsFactors = FALSE
  )
}

extract_elevation_fields <- function(df) {
  min_elev <- extract_numeric_candidate(df, c(
    "minimumelevationinmeters", "minimum_elevation_in_meters", "minimum_elevation_meters",
    "minimum_elevation", "minimumelevation", "elevation_min", "elev_min", "min_elev",
    "altitude_min", "minimumaltitude", "min_altitude"
  ))
  max_elev <- extract_numeric_candidate(df, c(
    "maximumelevationinmeters", "maximum_elevation_in_meters", "maximum_elevation_meters",
    "maximum_elevation", "maximumelevation", "elevation_max", "elev_max", "max_elev",
    "altitude_max", "maximumaltitude", "max_altitude"
  ))
  elev <- extract_numeric_candidate(df, c(
    "elevation", "elevation_m", "elevationmeters", "elevation_in_meters",
    "elev", "altitude", "alt", "alt_m", "altitude_m"
  ))
  verbatim_elev <- extract_character_candidate(df, c(
    "verbatimelevation", "verbatim_elevation", "elevation", "elevation_m",
    "elev", "altitude", "alt", "alt_m", "altitude_m"
  ))

  needs_min <- is.na(min_elev) & !is.na(elev)
  needs_max <- is.na(max_elev) & !is.na(elev)
  min_elev[needs_min] <- elev[needs_min]
  max_elev[needs_max] <- elev[needs_max]

  needs_max_from_min <- !is.na(min_elev) & is.na(max_elev)
  needs_min_from_max <- is.na(min_elev) & !is.na(max_elev)
  max_elev[needs_max_from_min] <- min_elev[needs_max_from_min]
  min_elev[needs_min_from_max] <- max_elev[needs_min_from_max]

  elev_out <- elev
  same_range <- is.na(elev_out) & !is.na(min_elev) & !is.na(max_elev) & (min_elev == max_elev)
  elev_out[same_range] <- min_elev[same_range]

  list(
    verbatimElevation = verbatim_elev,
    minimumElevationInMeters = min_elev,
    maximumElevationInMeters = max_elev,
    elevation = elev_out
  )
}

args <- parse_named_args(commandArgs(trailingOnly = TRUE))
root <- find_project_root()
paper_id <- args$`paper-id` %||% "jennings_2026"
raw_dir <- file.path(root, "data", "raw", paper_id)
interim_dir <- args$`interim-dir` %||% file.path(root, "data", "interim")
map_path <- args$`mapping-file` %||% file.path(root, "mappings", "jennings_2026_column_mapping.csv")
log_file <- file.path(root, "logs", paste0("normalize_", paper_id, ".log"))

if (!dir.exists(raw_dir)) stop("Missing raw directory: ", raw_dir, call. = FALSE)

papers <- read_papers_config(root)
paper <- papers[papers$paper_id == paper_id, , drop = FALSE]
if (!nrow(paper)) stop("paper_id not found in config: ", paper_id, call. = FALSE)

mapping <- NULL
if (file.exists(map_path)) {
  mapping <- tryCatch(
    utils::read.csv(map_path, stringsAsFactors = FALSE),
    error = function(e) {
      append_log(
        log_file,
        "WARN",
        "Mapping file unreadable; proceeding with heuristic extraction",
        paste("path=", map_path, " error=", conditionMessage(e))
      )
      NULL
    }
  )
  if (!is.null(mapping)) {
    required_cols <- c("source_column")
    missing_cols <- setdiff(required_cols, names(mapping))
    if (length(missing_cols)) {
      append_log(
        log_file,
        "WARN",
        "Mapping file missing required columns; proceeding with heuristic extraction",
        paste("path=", map_path, " missing=", paste(missing_cols, collapse = ","))
      )
      mapping <- NULL
    } else {
      mapping$source_column_norm <- normalize_name(mapping$source_column)
      append_log(log_file, "INFO", "Mapping file loaded (informational; heuristic extraction still active)", paste("rows=", nrow(mapping), " path=", map_path))
    }
  }
} else {
  append_log(log_file, "WARN", "Mapping file not found; proceeding with heuristic extraction", map_path)
}

files <- list.files(raw_dir, full.names = TRUE)
if (!length(files)) stop("No downloaded files in: ", raw_dir, call. = FALSE)

tabular_files <- files[grepl("\\.(xlsx|xls|csv|tsv|zip)$", files, ignore.case = TRUE)]
if (!length(tabular_files)) {
  append_log(log_file, "WARN", "No tabular files found", raw_dir)
  stop("No tabular files to normalize.", call. = FALSE)
}

rows_out <- list()
idx <- 0L

for (f in tabular_files) {
  ext <- tolower(tools::file_ext(f))
  if (ext == "zip") {
    # DwC-A archive — look for occurrence.txt (tab-delimited)
    zip_contents <- tryCatch(utils::unzip(f, list = TRUE)$Name, error = function(e) character(0))
    if (!"occurrence.txt" %in% zip_contents) {
      append_log(log_file, "WARN", "ZIP does not contain occurrence.txt; skipping", f)
      next
    }
    tmp_dir <- tempfile()
    dir.create(tmp_dir)
    tryCatch(utils::unzip(f, files = "occurrence.txt", exdir = tmp_dir), error = function(e) {
      append_log(log_file, "WARN", "Failed to extract occurrence.txt from ZIP",
                 paste(basename(f), conditionMessage(e)))
    })
    occ_path <- file.path(tmp_dir, "occurrence.txt")
    if (!file.exists(occ_path)) {
      unlink(tmp_dir, recursive = TRUE)
      next
    }
    df <- tryCatch(
      utils::read.table(occ_path, sep = "\t", header = TRUE, stringsAsFactors = FALSE,
                        quote = "", fill = TRUE, comment.char = ""),
      error = function(e) NULL
    )
    unlink(tmp_dir, recursive = TRUE)
    if (is.null(df) || !nrow(df)) next
    append_log(log_file, "INFO", "Loaded DwC-A occurrence.txt",
               paste("file=", basename(f), " rows=", nrow(df), " cols=", ncol(df)))
    idx <- idx + 1L
    rows_out[[idx]] <- build_normalized_block(
      df = df,
      paper = paper,
      paper_id = paper_id,
      source_file = basename(f),
      source_sheet = "occurrence"
    )
  } else if (ext %in% c("xlsx", "xls")) {
    sheet_names <- tryCatch(readxl::excel_sheets(f), error = function(e) character(0))
    if (!length(sheet_names)) {
      append_log(log_file, "WARN", "Unreadable workbook", f)
      next
    }
    for (sheet in sheet_names) {
      df <- tryCatch(as.data.frame(readxl::read_excel(f, sheet = sheet)), error = function(e) NULL)
      if (is.null(df) || !nrow(df)) next
      idx <- idx + 1L
      rows_out[[idx]] <- build_normalized_block(
        df = df,
        paper = paper,
        paper_id = paper_id,
        source_file = basename(f),
        source_sheet = sheet
      )
    }
  } else if (ext %in% c("csv", "tsv")) {
    sep <- if (ext == "tsv") "\t" else ","
    df <- tryCatch(utils::read.csv(f, sep = sep, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
    if (is.null(df) || !nrow(df)) next
    idx <- idx + 1L
    rows_out[[idx]] <- build_normalized_block(
      df = df,
      paper = paper,
      paper_id = paper_id,
      source_file = basename(f),
      source_sheet = ""
    )
  }
}

if (!length(rows_out)) stop("No tabular rows parsed from downloaded files.", call. = FALSE)

norm <- do.call(rbind, rows_out)
norm <- norm[!(is.na(norm$scientificName) | !nzchar(trimws(norm$scientificName))), , drop = FALSE]

if ("verbatimLatitude" %in% names(norm) && "verbatimLongitude" %in% names(norm)) {
  invalid_coord_rows <- sum((!is.na(norm$verbatimLatitude) & is.na(norm$decimalLatitude)) | (!is.na(norm$verbatimLongitude) & is.na(norm$decimalLongitude)))
  if (invalid_coord_rows > 0) {
    append_log(log_file, "WARN", "Coordinates outside valid range were set to NA in decimal fields", paste("rows=", invalid_coord_rows))
  }
}

out_path <- file.path(interim_dir, paste0(paper_id, "_dwc_normalized.csv"))
write_csv(norm, out_path)
append_log(log_file, "INFO", "Normalization complete", paste("rows=", nrow(norm), " output=", out_path))
message("Normalization complete: ", nrow(norm), " rows -> ", out_path)
