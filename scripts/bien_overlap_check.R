## bien_overlap_check.R
## Two-tier BIEN overlap screening:
##   Stage 1: paper-level fast screen on sampled names
##   Stage 2: targeted detail for uncertain papers
##
## Outputs:
##   output/bien_overlap_project_screen.csv
##   output/bien_overlap_name_detail.csv
##   output/bien_overlap_checkpoint_v2.csv
##   output/bien_overlap_per_species.csv
##   output/bien_overlap_per_paper.csv
##
## Usage:
##   Rscript scripts/bien_overlap_check.R
##   Rscript scripts/bien_overlap_check.R --stage1_sample_size=40 --batch_size=200 --force_stage2=FALSE

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(BIEN)
})

.args_all <- commandArgs(trailingOnly = FALSE)
.file_arg <- "--file="
.script_path <- sub(.file_arg, "", .args_all[grep(.file_arg, .args_all)][1])
project_root <- normalizePath(file.path(dirname(.script_path), ".."), winslash = "/", mustWork = FALSE)
source(file.path(project_root, "scripts", "utils.R"), local = FALSE)
project_root <- find_project_root()

args <- commandArgs(trailingOnly = TRUE)

get_arg_value <- function(arg_name, default_value) {
  prefix <- paste0("--", arg_name, "=")
  hit <- args[startsWith(args, prefix)]
  if (length(hit) == 0) return(default_value)
  sub(prefix, "", hit[[1]], fixed = TRUE)
}

parse_int_arg <- function(arg_name, default_value) {
  x <- suppressWarnings(as.integer(get_arg_value(arg_name, as.character(default_value))))
  if (is.na(x) || x <= 0) default_value else x
}

parse_bool_arg <- function(arg_name, default_value = FALSE) {
  x <- tolower(trimws(get_arg_value(arg_name, ifelse(default_value, "TRUE", "FALSE"))))
  x %in% c("true", "t", "1", "yes", "y")
}

stage1_sample_size <- parse_int_arg("stage1_sample_size", 40L)
batch_size <- parse_int_arg("batch_size", 200L)
force_stage2 <- parse_bool_arg("force_stage2", FALSE)

processed_dir <- file.path(project_root, "data", "processed")
config_papers_csv <- file.path(project_root, "config", "papers.csv")
output_dir <- file.path(project_root, "output")

checkpoint_csv <- file.path(output_dir, "bien_overlap_checkpoint_v2.csv")
project_screen_csv <- file.path(output_dir, "bien_overlap_project_screen.csv")
name_detail_csv <- file.path(output_dir, "bien_overlap_name_detail.csv")
per_species_csv <- file.path(output_dir, "bien_overlap_per_species.csv")
per_paper_csv <- file.path(output_dir, "bien_overlap_per_paper.csv")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

query_datetime_utc <- format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
bien_package_version <- as.character(utils::packageVersion("BIEN"))

get_bien_db_version <- function() {
  v <- tryCatch(BIEN::BIEN_metadata_database_version(), error = function(e) NA)
  if (is.null(v) || (length(v) == 1 && is.na(v))) return(NA_character_)
  if (is.data.frame(v)) {
    vals <- unlist(v, use.names = FALSE)
    vals <- vals[!is.na(vals)]
    if (length(vals) == 0) return(NA_character_)
    return(as.character(vals[[1]]))
  }
  vals <- unlist(v, use.names = FALSE)
  vals <- vals[!is.na(vals)]
  if (length(vals) == 0) NA_character_ else as.character(vals[[1]])
}

bien_db_version <- get_bien_db_version()

is_likely_binomial <- function(x) {
  x <- trimws(x)
  !is.na(x) &
    nchar(x) > 4 &
    grepl(" ", x) &
    !grepl("^[0-9]", x) &
    !grepl("\\bsp\\.?$", x, ignore.case = TRUE) &
    !grepl("\\bspp\\.?$", x, ignore.case = TRUE) &
    !grepl("\\bindet\\.?\\b", x, ignore.case = TRUE) &
    !grepl("\\bcf\\.?\\b", x, ignore.case = TRUE) &
    !grepl("MaterialCitation", x, fixed = TRUE)
}

ensure_cols <- function(df, cols) {
  for (nm in cols) {
    if (!nm %in% names(df)) df[[nm]] <- NA_character_
  }
  df
}

trim_or_na <- function(x) {
  y <- trimws(as.character(x))
  y[y == ""] <- NA_character_
  y
}

checkpoint_columns <- c(
  "name_for_query", "n_bien_records", "query_status", "query_error", "queried_at_utc",
  "bien_db_version", "bien_package_version", "query_datetime_utc"
)

load_checkpoint <- function(path) {
  if (!file.exists(path)) {
    return(tibble::tibble(
      name_for_query = character(),
      n_bien_records = integer(),
      query_status = character(),
      query_error = character(),
      queried_at_utc = character(),
      bien_db_version = character(),
      bien_package_version = character(),
      query_datetime_utc = character()
    ))
  }

  cp <- readr::read_csv(path, show_col_types = FALSE)
  cp <- ensure_cols(cp, checkpoint_columns)
  cp <- cp %>%
    dplyr::mutate(
      name_for_query = as.character(name_for_query),
      n_bien_records = suppressWarnings(as.integer(n_bien_records)),
      query_status = as.character(query_status),
      query_error = as.character(query_error),
      queried_at_utc = as.character(queried_at_utc),
      bien_db_version = as.character(bien_db_version),
      bien_package_version = as.character(bien_package_version),
      query_datetime_utc = as.character(query_datetime_utc)
    ) %>%
    dplyr::arrange(name_for_query, queried_at_utc) %>%
    dplyr::distinct(name_for_query, .keep_all = TRUE)

  cp
}

checkpoint <- load_checkpoint(checkpoint_csv)

query_bien_names <- function(name_vec, batch_size) {
  names_unique <- unique(trim_or_na(name_vec))
  names_unique <- names_unique[!is.na(names_unique)]

  if (length(names_unique) == 0) {
    return(checkpoint[0, checkpoint_columns])
  }

  existing <- checkpoint %>%
    dplyr::filter(name_for_query %in% names_unique)

  remaining <- setdiff(names_unique, existing$name_for_query)
  if (length(remaining) > 0) {
    n_batches <- ceiling(length(remaining) / batch_size)
    cat(sprintf("Querying BIEN for %d names in %d batches (size <= %d)\n",
                length(remaining), n_batches, batch_size))

    for (i in seq_len(n_batches)) {
      idx1 <- (i - 1) * batch_size + 1
      idx2 <- min(i * batch_size, length(remaining))
      batch <- remaining[idx1:idx2]
      queried_at_utc <- format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

      cat(sprintf("  Batch %d/%d: %d names...", i, n_batches, length(batch)))

      batch_rows <- tryCatch({
        res <- BIEN::BIEN_occurrence_records_per_species(batch)

        if (!is.null(res) && nrow(res) > 0) {
          if ("scrubbed_species_binomial_submitted" %in% names(res)) {
            key_col <- "scrubbed_species_binomial_submitted"
          } else if ("species" %in% names(res)) {
            key_col <- "species"
          } else if ("scrubbed_species_binomial" %in% names(res)) {
            key_col <- "scrubbed_species_binomial"
          } else {
            key_col <- NULL
          }

          counts <- tibble::tibble(name_for_query = batch, n_bien_records = 0L)

          if (!is.null(key_col) && "number_of_records" %in% names(res)) {
            mapped <- res %>%
              dplyr::transmute(
                name_for_query = as.character(.data[[key_col]]),
                n_bien_records = suppressWarnings(as.integer(number_of_records))
              ) %>%
              dplyr::filter(!is.na(name_for_query), !is.na(n_bien_records)) %>%
              dplyr::group_by(name_for_query) %>%
              dplyr::summarise(n_bien_records = max(n_bien_records, na.rm = TRUE), .groups = "drop")

            counts <- counts %>%
              dplyr::left_join(mapped, by = "name_for_query", suffix = c("", "_mapped")) %>%
              dplyr::mutate(n_bien_records = dplyr::coalesce(n_bien_records_mapped, n_bien_records)) %>%
              dplyr::select(name_for_query, n_bien_records)
          }

          counts %>%
            dplyr::mutate(
              query_status = "ok",
              query_error = NA_character_,
              queried_at_utc = queried_at_utc,
              bien_db_version = bien_db_version,
              bien_package_version = bien_package_version,
              query_datetime_utc = query_datetime_utc
            )
        } else {
          tibble::tibble(
            name_for_query = batch,
            n_bien_records = 0L,
            query_status = "ok",
            query_error = NA_character_,
            queried_at_utc = queried_at_utc,
            bien_db_version = bien_db_version,
            bien_package_version = bien_package_version,
            query_datetime_utc = query_datetime_utc
          )
        }
      }, error = function(e) {
        tibble::tibble(
          name_for_query = batch,
          n_bien_records = NA_integer_,
          query_status = "error",
          query_error = as.character(conditionMessage(e)),
          queried_at_utc = queried_at_utc,
          bien_db_version = bien_db_version,
          bien_package_version = bien_package_version,
          query_datetime_utc = query_datetime_utc
        )
      })

      checkpoint <<- dplyr::bind_rows(checkpoint, batch_rows) %>%
        dplyr::arrange(name_for_query, queried_at_utc) %>%
        dplyr::distinct(name_for_query, .keep_all = TRUE)

      cat(" done\n")
    }

    readr::write_csv(checkpoint %>% dplyr::select(dplyr::all_of(checkpoint_columns)), checkpoint_csv)
  }

  checkpoint %>%
    dplyr::filter(name_for_query %in% names_unique) %>%
    dplyr::select(dplyr::all_of(checkpoint_columns))
}

staging_files <- list.files(processed_dir, pattern = "_bien_staging\\.csv$", full.names = TRUE)
if (length(staging_files) == 0) {
  stop("No *_bien_staging.csv files found in ", processed_dir)
}

cat(sprintf("Found %d staging files\n", length(staging_files)))

required_stage_fields <- c(
  "name_submitted", "tnrs_matched_name", "taxon_scrub_status",
  "source_citation", "source_url", "source_file", "source_sheet"
)

raw_list <- lapply(staging_files, function(f) {
  df <- tryCatch(readr::read_csv(f, show_col_types = FALSE), error = function(e) NULL)
  if (is.null(df)) {
    warning("Could not read: ", f)
    return(NULL)
  }

  paper_id <- sub("_bien_staging\\.csv$", "", basename(f))
  df <- ensure_cols(df, required_stage_fields)

  df %>%
    dplyr::transmute(
      paper_id = paper_id,
      name_submitted = as.character(name_submitted),
      tnrs_matched_name = as.character(tnrs_matched_name),
      taxon_scrub_status = as.character(taxon_scrub_status),
      source_citation = as.character(source_citation),
      source_url = as.character(source_url),
      source_file = as.character(source_file),
      source_sheet = as.character(source_sheet)
    )
})

all_raw <- dplyr::bind_rows(raw_list)
cat(sprintf("Total rows across all staging files: %d\n", nrow(all_raw)))

all_candidates <- all_raw %>%
  dplyr::mutate(
    name_submitted = trim_or_na(name_submitted),
    tnrs_matched_name = trim_or_na(tnrs_matched_name),
    name_for_query = dplyr::if_else(!is.na(tnrs_matched_name), tnrs_matched_name, name_submitted)
  ) %>%
  dplyr::filter(!is.na(name_for_query)) %>%
  dplyr::filter(is_likely_binomial(name_for_query)) %>%
  dplyr::distinct(
    paper_id, name_submitted, name_for_query, tnrs_matched_name,
    taxon_scrub_status, source_citation, source_url, source_file, source_sheet
  )

cat(sprintf("Likely binomial paper/name rows: %d\n", nrow(all_candidates)))

paper_name_unique <- all_candidates %>%
  dplyr::group_by(paper_id, name_for_query) %>%
  dplyr::slice_head(n = 1) %>%
  dplyr::ungroup()

set.seed(42)
stage1_samples <- paper_name_unique %>%
  dplyr::group_by(paper_id) %>%
  dplyr::group_modify(~ dplyr::slice_sample(.x, n = min(nrow(.x), stage1_sample_size))) %>%
  dplyr::ungroup()

stage1_counts <- query_bien_names(stage1_samples$name_for_query, batch_size)

stage1_detail <- stage1_samples %>%
  dplyr::left_join(stage1_counts, by = "name_for_query")

stage1_summary <- stage1_detail %>%
  dplyr::group_by(paper_id) %>%
  dplyr::summarise(
    stage1_sampled = dplyr::n_distinct(name_for_query),
    stage1_queried_ok = sum(query_status == "ok", na.rm = TRUE),
    stage1_query_errors = sum(query_status == "error", na.rm = TRUE),
    stage1_with_records = sum(query_status == "ok" & n_bien_records > 0, na.rm = TRUE),
    pct_sample_with_records = dplyr::if_else(
      stage1_queried_ok > 0,
      round(100 * stage1_with_records / stage1_queried_ok, 1),
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  dplyr::mutate(
    stage1_status = dplyr::case_when(
      stage1_sampled < 10 ~ "unknown_needs_detail",
      stage1_query_errors > 0 ~ "unknown_needs_detail",
      !is.na(pct_sample_with_records) & pct_sample_with_records >= 70 ~ "confirmed_overlap",
      !is.na(pct_sample_with_records) & pct_sample_with_records >= 30 ~ "probable_overlap",
      !is.na(pct_sample_with_records) & pct_sample_with_records < 30 ~ "likely_absent",
      TRUE ~ "unknown_needs_detail"
    ),
    needs_stage2 = stage1_status %in% c("probable_overlap", "unknown_needs_detail")
  )

papers_meta <- if (file.exists(config_papers_csv)) {
  readr::read_csv(config_papers_csv, show_col_types = FALSE) %>%
    dplyr::select(dplyr::any_of(c("paper_id", "doi", "citation", "year", "journal", "publisher")))
} else {
  tibble::tibble(
    paper_id = unique(all_candidates$paper_id),
    doi = NA_character_,
    citation = NA_character_,
    year = NA_real_,
    journal = NA_character_,
    publisher = NA_character_
  )
}

project_screen <- stage1_summary %>%
  dplyr::left_join(papers_meta, by = "paper_id") %>%
  dplyr::mutate(
    bien_db_version = bien_db_version,
    bien_package_version = bien_package_version,
    query_datetime_utc = query_datetime_utc
  ) %>%
  dplyr::select(
    paper_id, doi, citation, year, journal, publisher,
    stage1_sampled, stage1_queried_ok, stage1_query_errors, stage1_with_records,
    pct_sample_with_records, stage1_status, needs_stage2,
    bien_db_version, bien_package_version, query_datetime_utc
  ) %>%
  dplyr::arrange(paper_id)

readr::write_csv(project_screen, project_screen_csv)
cat(sprintf("Wrote %s (%d rows)\n", project_screen_csv, nrow(project_screen)))

stage2_target_papers <- if (isTRUE(force_stage2)) {
  unique(project_screen$paper_id)
} else {
  project_screen %>%
    dplyr::filter(stage1_status %in% c("probable_overlap", "unknown_needs_detail")) %>%
    dplyr::pull(paper_id)
}

stage2_rows <- paper_name_unique %>%
  dplyr::filter(paper_id %in% stage2_target_papers)

stage2_counts <- query_bien_names(stage2_rows$name_for_query, batch_size)

stage1_detail_rows <- stage1_samples %>%
  dplyr::left_join(stage1_counts, by = "name_for_query") %>%
  dplyr::mutate(overlap_tier = "stage1_sample")

stage2_detail_rows <- stage2_rows %>%
  dplyr::left_join(stage2_counts, by = "name_for_query") %>%
  dplyr::mutate(overlap_tier = "stage2_detail")

name_detail <- dplyr::bind_rows(stage1_detail_rows, stage2_detail_rows) %>%
  dplyr::arrange(paper_id, name_for_query, dplyr::desc(overlap_tier == "stage2_detail")) %>%
  dplyr::distinct(paper_id, name_for_query, .keep_all = TRUE) %>%
  dplyr::mutate(
    in_bien = query_status == "ok" & n_bien_records > 0
  ) %>%
  dplyr::select(
    paper_id,
    name_submitted,
    name_for_query,
    tnrs_matched_name,
    taxon_scrub_status,
    overlap_tier,
    n_bien_records,
    in_bien,
    query_status,
    query_error,
    queried_at_utc,
    bien_db_version,
    bien_package_version,
    query_datetime_utc
  ) %>%
  dplyr::arrange(paper_id, dplyr::desc(in_bien), name_for_query)

readr::write_csv(name_detail, name_detail_csv)
cat(sprintf("Wrote %s (%d rows)\n", name_detail_csv, nrow(name_detail)))

species_papers <- all_candidates %>%
  dplyr::group_by(name_submitted) %>%
  dplyr::summarise(
    paper_ids = paste(sort(unique(paper_id)), collapse = "; "),
    .groups = "drop"
  )

species_query <- name_detail %>%
  dplyr::group_by(name_submitted) %>%
  dplyr::summarise(
    n_bien_records = dplyr::if_else(
      any(query_status == "ok"),
      max(n_bien_records[query_status == "ok"], na.rm = TRUE),
      NA_integer_
    ),
    in_bien = any(query_status == "ok" & n_bien_records > 0, na.rm = TRUE),
    queried_any = any(query_status %in% c("ok", "error"), na.rm = TRUE),
    overlap_evidence_tier = dplyr::case_when(
      any(overlap_tier == "stage2_detail") ~ "stage2_detail",
      any(overlap_tier == "stage1_sample") ~ "stage1_sample",
      TRUE ~ NA_character_
    ),
    .groups = "drop"
  )

per_species <- species_papers %>%
  dplyr::left_join(species_query, by = "name_submitted") %>%
  dplyr::mutate(
    in_bien = dplyr::coalesce(in_bien, FALSE),
    queried_any = dplyr::coalesce(queried_any, FALSE),
    overlap_evidence_tier = dplyr::coalesce(overlap_evidence_tier, "none")
  ) %>%
  dplyr::select(name_submitted, paper_ids, n_bien_records, in_bien, queried_any, overlap_evidence_tier) %>%
  dplyr::arrange(dplyr::desc(in_bien), name_submitted)

readr::write_csv(per_species, per_species_csv)
cat(sprintf("Wrote %s (%d rows)\n", per_species_csv, nrow(per_species)))

paper_submitted <- all_candidates %>%
  dplyr::group_by(paper_id) %>%
  dplyr::summarise(n_species_submitted = dplyr::n_distinct(name_submitted), .groups = "drop")

paper_queried <- name_detail %>%
  dplyr::group_by(paper_id) %>%
  dplyr::summarise(
    n_species_queried = dplyr::n_distinct(name_for_query[query_status == "ok"]),
    n_in_bien = dplyr::n_distinct(name_for_query[query_status == "ok" & n_bien_records > 0]),
    n_not_in_bien = dplyr::n_distinct(name_for_query[query_status == "ok" & n_bien_records == 0]),
    pct_in_bien_among_queried = dplyr::if_else(
      n_species_queried > 0,
      round(100 * n_in_bien / n_species_queried, 1),
      NA_real_
    ),
    total_bien_records = sum(n_bien_records[query_status == "ok"], na.rm = TRUE),
    .groups = "drop"
  )

per_paper <- paper_submitted %>%
  dplyr::left_join(paper_queried, by = "paper_id") %>%
  dplyr::left_join(project_screen %>% dplyr::select(paper_id, stage1_status, needs_stage2), by = "paper_id") %>%
  dplyr::mutate(
    n_species_queried = dplyr::coalesce(n_species_queried, 0L),
    n_in_bien = dplyr::coalesce(n_in_bien, 0L),
    n_not_in_bien = dplyr::coalesce(n_not_in_bien, 0L),
    total_bien_records = dplyr::coalesce(total_bien_records, 0L),
    needs_stage2 = dplyr::coalesce(needs_stage2, FALSE)
  ) %>%
  dplyr::select(
    paper_id,
    n_species_submitted,
    n_species_queried,
    n_in_bien,
    n_not_in_bien,
    pct_in_bien_among_queried,
    stage1_status,
    needs_stage2,
    total_bien_records
  ) %>%
  dplyr::arrange(dplyr::desc(pct_in_bien_among_queried), paper_id)

readr::write_csv(per_paper, per_paper_csv)
cat(sprintf("Wrote %s (%d rows)\n", per_paper_csv, nrow(per_paper)))

cat("\nCaveat: overlap metrics are screening metrics and should not be interpreted as definitive evidence of species absence from BIEN.\n")
cat("Done.\n")
