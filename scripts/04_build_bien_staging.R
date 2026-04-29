#!/usr/bin/env Rscript

args_all <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_path <- sub(file_arg, "", args_all[grep(file_arg, args_all)][1])
project_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
source(file.path(project_root, "scripts", "utils.R"), local = FALSE)

args <- parse_named_args(commandArgs(trailingOnly = TRUE))
root <- find_project_root()
paper_id <- args$`paper-id` %||% "jennings_2026"
interim_dir <- args$`interim-dir` %||% file.path(root, "data", "interim")
processed_dir <- args$`processed-dir` %||% file.path(root, "data", "processed")
log_file <- file.path(root, "logs", paste0("staging_", paper_id, ".log"))

in_path <- file.path(interim_dir, paste0(paper_id, "_dwc_normalized.csv"))
if (!file.exists(in_path)) stop("Missing normalized table: ", in_path, call. = FALSE)

df <- utils::read.csv(in_path, stringsAsFactors = FALSE, check.names = FALSE)
if (!nrow(df)) stop("Normalized table is empty: ", in_path, call. = FALSE)

get_chr <- function(data, col) {
  if (!col %in% names(data)) return(rep(NA_character_, nrow(data)))
  out <- as.character(data[[col]])
  out[!nzchar(trimws(out))] <- NA_character_
  out
}

get_num <- function(data, col) {
  if (!col %in% names(data)) return(rep(NA_real_, nrow(data)))
  safe_numeric(data[[col]])
}

elevation_point <- safe_numeric(df$elevation)
if (!"elevation" %in% names(df)) elevation_point <- rep(NA_real_, nrow(df))
if ("minimumElevationInMeters" %in% names(df) && "maximumElevationInMeters" %in% names(df)) {
  min_e <- safe_numeric(df$minimumElevationInMeters)
  max_e <- safe_numeric(df$maximumElevationInMeters)
  same_range <- is.na(elevation_point) & !is.na(min_e) & !is.na(max_e) & (min_e == max_e)
  elevation_point[same_range] <- min_e[same_range]
}

verbatim_elev <- if ("verbatimElevation" %in% names(df)) as.character(df$verbatimElevation) else rep(NA_character_, nrow(df))

state_province <- get_chr(df, "stateProvince")
county <- get_chr(df, "county")
municipality <- get_chr(df, "municipality")
country <- get_chr(df, "country")
country_code <- get_chr(df, "countryCode")
locality <- get_chr(df, "locality")
verbatim_locality <- get_chr(df, "verbatimLocality")
locality_notes <- get_chr(df, "localityNotes")
island <- get_chr(df, "island")
island_group <- get_chr(df, "islandGroup")
water_body <- get_chr(df, "waterBody")
event_year <- suppressWarnings(as.integer(get_chr(df, "eventYear")))
event_month <- suppressWarnings(as.integer(get_chr(df, "eventMonth")))
event_day <- suppressWarnings(as.integer(get_chr(df, "eventDay")))
habit_values <- get_chr(df, "habit")

staging <- data.frame(
  observation_id = paste0(paper_id, "_", seq_len(nrow(df))),
  occurrence_id = get_chr(df, "occurrenceID"),
  name_submitted = df$scientificName,
  family = df$family,
  genus = df$genus,
  latitude = get_num(df, "decimalLatitude"),
  longitude = get_num(df, "decimalLongitude"),
  decimalLatitude = get_num(df, "decimalLatitude"),
  decimalLongitude = get_num(df, "decimalLongitude"),
  country = country,
  country_code = country_code,
  state_province = state_province,
  county_parish = county,
  municipality = municipality,
  country_raw = country,
  state_province_raw = state_province,
  county_raw = county,
  municipality_raw = municipality,
  political_units_raw = get_chr(df, "politicalUnitsRaw"),
  locality = locality,
  verbatim_locality = verbatim_locality,
  locality_notes = locality_notes,
  island = island,
  island_group = island_group,
  water_body = water_body,
  date_collected = get_chr(df, "eventDate"),
  event_year = event_year,
  event_month = event_month,
  event_day = event_day,
  collector = get_chr(df, "recordedBy"),
  catalog_number = get_chr(df, "catalogNumber"),
  record_number = get_chr(df, "recordNumber"),
  basis_of_record = get_chr(df, "basisOfRecord"),
  occurrence_status = get_chr(df, "occurrenceStatus"),
  establishment_means = get_chr(df, "establishmentMeans"),
  verbatim_elevation = verbatim_elev,
  elevation_m = elevation_point,
  elevation_min_m = get_num(df, "minimumElevationInMeters"),
  elevation_max_m = get_num(df, "maximumElevationInMeters"),
  coordinate_uncertainty_m = get_num(df, "coordinateUncertaintyInMeters"),
  geodetic_datum = get_chr(df, "geodeticDatum"),
  georeference_remarks = get_chr(df, "georeferenceRemarks"),
  habitat = get_chr(df, "habitat"),
  habit_raw = habit_values,
  occurrence_remarks = get_chr(df, "occurrenceRemarks"),
  source_citation = get_chr(df, "citation"),
  source_url = get_chr(df, "source_url"),
  source_file = get_chr(df, "source_file"),
  source_sheet = get_chr(df, "source_sheet"),
  original_row_number = df$original_row_number,
  taxon_scrub_status = "UNRESOLVED",
  tnrs_matched_name = NA_character_,
  gnrs_status = NA_character_,
  gvs_status = NA_character_,
  nsr_native_status = NA_character_,
  stringsAsFactors = FALSE
)

staging <- staging[!(is.na(staging$name_submitted) | !nzchar(trimws(staging$name_submitted))), , drop = FALSE]
has_habit <- !is.na(staging$habit_raw) & nzchar(trimws(staging$habit_raw))
habit_n <- sum(has_habit)

if (!"occurrence_id" %in% names(staging) || all(is.na(staging$occurrence_id))) {
  staging$occurrence_id <- paste0(staging$observation_id, "_occ")
}

out_path <- file.path(processed_dir, paste0(paper_id, "_bien_staging.csv"))
staging_out <- staging
staging_out$habit_raw <- NULL
write_csv(staging_out, out_path)

trait_path <- file.path(processed_dir, paste0(paper_id, "_habit_traits.csv"))

if (habit_n > 0) {
  trait_sidecar <- data.frame(
    trait_observation_id = paste0(staging$observation_id[has_habit], "_habit"),
    observation_id = staging$observation_id[has_habit],
    occurrence_id = staging$occurrence_id[has_habit],
    paper_id = paper_id,
    scientific_name = staging$name_submitted[has_habit],
    trait_name = "growth_form",
    trait_value = staging$habit_raw[has_habit],
    trait_value_standardized = tolower(trimws(staging$habit_raw[has_habit])),
    source_citation = staging$source_citation[has_habit],
    source_url = staging$source_url[has_habit],
    source_file = staging$source_file[has_habit],
    source_sheet = staging$source_sheet[has_habit],
    original_row_number = staging$original_row_number[has_habit],
    stringsAsFactors = FALSE
  )
} else {
  trait_sidecar <- data.frame(
    trait_observation_id = character(0),
    observation_id = character(0),
    occurrence_id = character(0),
    paper_id = character(0),
    scientific_name = character(0),
    trait_name = character(0),
    trait_value = character(0),
    trait_value_standardized = character(0),
    source_citation = character(0),
    source_url = character(0),
    source_file = character(0),
    source_sheet = character(0),
    original_row_number = integer(0),
    stringsAsFactors = FALSE
  )
}

write_csv(trait_sidecar, trait_path)

summary_path <- file.path(processed_dir, paste0(paper_id, "_staging_summary.csv"))
summary <- data.frame(
  paper_id = paper_id,
  n_rows = nrow(staging),
  n_with_coords = sum(!is.na(staging$latitude) & !is.na(staging$longitude)),
  n_with_country = sum(!is.na(staging$country) & nzchar(staging$country)),
  n_with_state_province = sum(!is.na(staging$state_province) & nzchar(staging$state_province)),
  n_with_county_parish = sum(!is.na(staging$county_parish) & nzchar(staging$county_parish)),
  n_with_municipality = sum(!is.na(staging$municipality) & nzchar(staging$municipality)),
  n_habit_trait_rows = habit_n,
  n_unique_species = length(unique(staging$name_submitted)),
  generated_at_utc = timestamp_utc(),
  stringsAsFactors = FALSE
)
write_csv(summary, summary_path)

append_log(log_file, "INFO", "Staging build complete", paste("rows=", nrow(staging), " output=", out_path))
append_log(log_file, "INFO", "Habit trait sidecar complete", paste("rows=", habit_n, " output=", trait_path))
if (habit_n == 0) {
  append_log(log_file, "INFO", "No explicit habit metadata found in normalized input", in_path)
}
message("Staging build complete: ", nrow(staging), " rows -> ", out_path)
