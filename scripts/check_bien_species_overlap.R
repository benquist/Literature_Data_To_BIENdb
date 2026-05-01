#!/usr/bin/env Rscript
# scripts/check_bien_species_overlap.R
# Query BIEN for species presence in each *_bien_staging.csv, produce overlap
# outputs in output/bien_species_overlap_detail.csv and
# output/bien_species_overlap_summary.csv.
#
# Usage:
#   Rscript scripts/check_bien_species_overlap.R          # uses checkpoint
#   Rscript scripts/check_bien_species_overlap.R --force  # re-queries BIEN

suppressPackageStartupMessages({
  library(BIEN)
  library(dplyr)
  library(readr)
})

# ── Paths ────────────────────────────────────────────────────────────────────
args         <- commandArgs(trailingOnly = TRUE)
force_rerun  <- "--force" %in% args

# Derive project root: script lives at <project>/scripts/check_bien_species_overlap.R
script_args  <- commandArgs(trailingOnly = FALSE)
file_flag    <- grep("^--file=", script_args, value = TRUE)
if (length(file_flag) > 0) {
  script_path  <- normalizePath(sub("^--file=", "", file_flag[1]))
  project_root <- dirname(dirname(script_path))   # up from scripts/
} else if (nzchar(Sys.getenv("PROJECT_ROOT"))) {
  project_root <- normalizePath(Sys.getenv("PROJECT_ROOT"))
} else {
  project_root <- normalizePath(getwd())
}
cat(sprintf("Project root: %s\n", project_root))

processed_dir  <- file.path(project_root, "data", "processed")
output_dir     <- file.path(project_root, "output")
detail_file    <- file.path(output_dir, "bien_species_overlap_detail.csv")
summary_file   <- file.path(output_dir, "bien_species_overlap_summary.csv")

if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# ── Step 1: Read all staging files ───────────────────────────────────────────
staging_files <- list.files(processed_dir, pattern = "_bien_staging\\.csv$",
                            full.names = TRUE)
if (length(staging_files) == 0) {
  stop("No *_bien_staging.csv files found in ", processed_dir)
}
cat(sprintf("Found %d staging files.\n", length(staging_files)))

staging_list <- lapply(staging_files, function(f) {
  df <- tryCatch(readr::read_csv(f, show_col_types = FALSE),
                 error = function(e) { cat("  WARNING: Could not read", f, "\n"); NULL })
  if (is.null(df)) return(NULL)
  if (!"name_submitted" %in% names(df)) {
    cat("  WARNING: name_submitted missing in", basename(f), "\n")
    return(NULL)
  }
  # derive paper_id from file basename
  paper_id <- sub("_bien_staging\\.csv$", "", basename(f))
  df$paper_id <- paper_id
  df[, c("name_submitted", "paper_id")]
})
staging_combined <- do.call(rbind, Filter(Negate(is.null), staging_list))
cat(sprintf("Total rows loaded: %d\n", nrow(staging_combined)))

# ── Step 2: Extract unique valid binomials ────────────────────────────────────
staging_combined$name_submitted <- trimws(as.character(staging_combined$name_submitted))
staging_combined <- staging_combined[
  !is.na(staging_combined$name_submitted) &
  nchar(staging_combined$name_submitted) > 0 &
  grepl(" ", staging_combined$name_submitted), ]

# Build species–paper lookup (which papers each species appears in)
species_paper <- staging_combined %>%
  dplyr::distinct(name_submitted, paper_id) %>%
  dplyr::group_by(name_submitted) %>%
  dplyr::summarise(papers_present_in = paste(sort(unique(paper_id)), collapse = "; "),
                   .groups = "drop")

all_species <- sort(unique(staging_combined$name_submitted))
cat(sprintf("Unique valid binomials: %d\n", length(all_species)))

# ── Step 3: Checkpoint ────────────────────────────────────────────────────────
if (!force_rerun && file.exists(detail_file)) {
  detail_existing <- tryCatch(readr::read_csv(detail_file, show_col_types = FALSE),
                              error = function(e) NULL)
  if (!is.null(detail_existing) && nrow(detail_existing) > 0) {
    cat("Checkpoint found: ", detail_file, " (", nrow(detail_existing), " rows). ",
        "Skipping BIEN query. Use --force to override.\n", sep = "")
    detail_df <- detail_existing
    do_query  <- FALSE
  } else {
    do_query  <- TRUE
  }
} else {
  do_query <- TRUE
}

# ── Step 4: Batch-query BIEN ──────────────────────────────────────────────────
if (do_query) {
  batch_size  <- 200L
  batches     <- split(all_species,
                       ceiling(seq_along(all_species) / batch_size))
  n_batches   <- length(batches)
  cat(sprintf("Querying BIEN in %d batches of up to %d species each ...\n",
              n_batches, batch_size))

  bien_results <- vector("list", n_batches)
  for (i in seq_len(n_batches)) {
    if (i %% 10 == 0 || i == 1 || i == n_batches) {
      cat(sprintf("  Batch %d of %d ...\n", i, n_batches))
    }
    batch <- batches[[i]]
    res   <- tryCatch(
      BIEN::BIEN_occurrence_records_per_species(batch),
      error = function(e) {
        cat(sprintf("  WARNING batch %d error: %s\n", i, conditionMessage(e)))
        NULL
      }
    )
    if (!is.null(res) && nrow(res) > 0) {
      bien_results[[i]] <- res
    }
    Sys.sleep(1)
  }

  bien_found <- do.call(rbind, Filter(Negate(is.null), bien_results))

  # Build detail data.frame
  if (!is.null(bien_found) && nrow(bien_found) > 0) {
    bien_found_clean <- bien_found %>%
      dplyr::rename(species = scrubbed_species_binomial,
                    bien_record_count = count) %>%
      dplyr::mutate(species = trimws(as.character(species)),
                    bien_record_count = as.integer(bien_record_count))
  } else {
    bien_found_clean <- data.frame(species = character(0),
                                   bien_record_count = integer(0),
                                   stringsAsFactors = FALSE)
  }

  detail_df <- data.frame(species = all_species, stringsAsFactors = FALSE) %>%
    dplyr::left_join(bien_found_clean, by = "species") %>%
    dplyr::mutate(
      in_bien           = !is.na(bien_record_count),
      bien_record_count = ifelse(is.na(bien_record_count), 0L, as.integer(bien_record_count))
    ) %>%
    dplyr::left_join(species_paper, by = c("species" = "name_submitted"))

  readr::write_csv(detail_df, detail_file)
  cat(sprintf("Wrote %s (%d rows)\n", detail_file, nrow(detail_df)))
}

# ── Step 5: Build paper-level summary ────────────────────────────────────────
# For each paper, count unique species in that paper and how many are in BIEN

# Rebuild species–paper mapping from staging (always recompute summary)
species_per_paper <- staging_combined %>%
  dplyr::distinct(name_submitted, paper_id)

# Join BIEN status onto per-paper species list
species_bien_status <- detail_df %>%
  dplyr::select(species, in_bien, bien_record_count)

paper_summary <- species_per_paper %>%
  dplyr::left_join(species_bien_status,
                   by = c("name_submitted" = "species")) %>%
  dplyr::group_by(paper_id) %>%
  dplyr::summarise(
    total_species    = dplyr::n(),
    in_bien_count    = sum(in_bien, na.rm = TRUE),
    not_in_bien_count = sum(!in_bien, na.rm = TRUE),
    pct_in_bien      = round(in_bien_count / total_species * 100, 1),
    .groups = "drop"
  ) %>%
  dplyr::arrange(desc(total_species))

readr::write_csv(paper_summary, summary_file)
cat(sprintf("Wrote %s (%d rows)\n", summary_file, nrow(paper_summary)))

# ── Step 6: Console summary ───────────────────────────────────────────────────
total_spp     <- nrow(detail_df)
total_in      <- sum(detail_df$in_bien, na.rm = TRUE)
total_out     <- total_spp - total_in
overall_pct   <- round(total_in / max(total_spp, 1) * 100, 1)

cat("\n========== BIEN SPECIES OVERLAP SUMMARY ==========\n")
cat(sprintf("Total unique species queried : %d\n", total_spp))
cat(sprintf("In BIEN                      : %d (%.1f%%)\n", total_in, overall_pct))
cat(sprintf("Not in BIEN                  : %d (%.1f%%)\n", total_out, 100 - overall_pct))
cat("\nPer-paper breakdown:\n")
print(as.data.frame(paper_summary), row.names = FALSE)
cat("===================================================\n")
