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

staging <- data.frame(
  observation_id = paste0(paper_id, "_", seq_len(nrow(df))),
  name_submitted = df$scientificName,
  family = df$family,
  genus = df$genus,
  latitude = safe_numeric(df$decimalLatitude),
  longitude = safe_numeric(df$decimalLongitude),
  country = df$country,
  state_province = NA_character_,
  county_parish = NA_character_,
  locality = df$locality,
  island = df$island,
  date_collected = df$eventDate,
  collector = df$recordedBy,
  catalog_number = df$catalogNumber,
  basis_of_record = df$basisOfRecord,
  elevation_min_m = safe_numeric(df$minimumElevationInMeters),
  elevation_max_m = safe_numeric(df$maximumElevationInMeters),
  coordinate_uncertainty_m = safe_numeric(df$coordinateUncertaintyInMeters),
  occurrence_remarks = df$occurrenceRemarks,
  source_citation = df$citation,
  source_url = df$source_url,
  source_file = df$source_file,
  source_sheet = df$source_sheet,
  original_row_number = df$original_row_number,
  taxon_scrub_status = "UNRESOLVED",
  tnrs_matched_name = NA_character_,
  gnrs_status = NA_character_,
  gvs_status = NA_character_,
  nsr_native_status = NA_character_,
  stringsAsFactors = FALSE
)

staging <- staging[!(is.na(staging$name_submitted) | !nzchar(trimws(staging$name_submitted))), , drop = FALSE]
out_path <- file.path(processed_dir, paste0(paper_id, "_bien_staging.csv"))
write_csv(staging, out_path)

summary_path <- file.path(processed_dir, paste0(paper_id, "_staging_summary.csv"))
summary <- data.frame(
  paper_id = paper_id,
  n_rows = nrow(staging),
  n_with_coords = sum(!is.na(staging$latitude) & !is.na(staging$longitude)),
  n_unique_species = length(unique(staging$name_submitted)),
  generated_at_utc = timestamp_utc(),
  stringsAsFactors = FALSE
)
write_csv(summary, summary_path)

append_log(log_file, "INFO", "Staging build complete", paste("rows=", nrow(staging), " output=", out_path))
message("Staging build complete: ", nrow(staging), " rows -> ", out_path)
