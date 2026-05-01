## bien_overlap_check.R
## Checks how many literature species are in the BIEN database.
## Outputs:
##   output/bien_overlap_per_species.csv
##   output/bien_overlap_per_paper.csv
## Checkpoints partial results to output/bien_overlap_checkpoint.csv
## Usage: Rscript scripts/bien_overlap_check.R

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(BIEN)
})

project_root <- "/Users/brianjenquist/VSCode/Literature_Data_To_BIENdb"

processed_dir  <- file.path(project_root, "data", "processed")
output_dir     <- file.path(project_root, "output")
checkpoint_csv <- file.path(output_dir, "bien_overlap_checkpoint.csv")
per_species_csv <- file.path(output_dir, "bien_overlap_per_species.csv")
per_paper_csv  <- file.path(output_dir, "bien_overlap_per_paper.csv")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# ── 1. Read all staging files ──────────────────────────────────────────────
staging_files <- list.files(processed_dir, pattern = "_bien_staging\\.csv$",
                            full.names = TRUE)
if (length(staging_files) == 0) stop("No *_bien_staging.csv files found in ", processed_dir)

cat(sprintf("Found %d staging files\n", length(staging_files)))

raw_list <- lapply(staging_files, function(f) {
  df <- tryCatch(readr::read_csv(f, show_col_types = FALSE), error = function(e) NULL)
  if (is.null(df)) { warning("Could not read: ", f); return(NULL) }
  if (!"name_submitted" %in% names(df)) { warning("No name_submitted in: ", f); return(NULL) }
  paper_id <- sub("_bien_staging\\.csv$", "", basename(f))
  df %>%
    dplyr::select(name_submitted) %>%
    dplyr::mutate(paper_id = paper_id)
})
all_raw <- dplyr::bind_rows(raw_list)

cat(sprintf("Total rows across all staging files: %d\n", nrow(all_raw)))

# ── 2. Filter to probable binomials ───────────────────────────────────────
non_binomial_patterns <- c(
  "\\bsp\\b", "\\bsp\\.$", "\\bspp\\b", "\\bindet\\b", "\\bcf\\b",
  "MaterialCitation", "^[0-9]"
)

is_likely_binomial <- function(x) {
  x <- trimws(x)
  !is.na(x) &
  nchar(x) > 4 &
  grepl(" ", x) &                          # must contain a space
  !grepl("^[0-9]", x) &                   # must not start with digit
  !grepl("\\bsp\\.?$", x, ignore.case = TRUE) &
  !grepl("\\bspp\\.?$", x, ignore.case = TRUE) &
  !grepl("\\bindet\\.?\\b", x, ignore.case = TRUE) &
  !grepl("\\bcf\\.?\\b", x, ignore.case = TRUE) &
  !grepl("MaterialCitation", x, fixed = TRUE)
}

binomials <- all_raw %>%
  dplyr::filter(is_likely_binomial(name_submitted)) %>%
  dplyr::distinct(name_submitted, paper_id)

cat(sprintf("Probable binomials (unique name×paper combos): %d\n", nrow(binomials)))

all_species <- unique(binomials$name_submitted)
cat(sprintf("Unique binomial names across all papers: %d\n", length(all_species)))

# ── 3. Checkpoint/resume ───────────────────────────────────────────────────
if (file.exists(checkpoint_csv)) {
  checkpoint <- readr::read_csv(checkpoint_csv, show_col_types = FALSE)
  already_queried <- unique(checkpoint$scrubbed_species_binomial_submitted)
  remaining <- setdiff(all_species, already_queried)
  cat(sprintf("Checkpoint found: %d already queried, %d remaining\n",
              length(already_queried), length(remaining)))
} else {
  checkpoint <- NULL
  remaining  <- all_species
}

# ── 4. Batch query BIEN ────────────────────────────────────────────────────
batch_size <- 100
n_batches  <- ceiling(length(remaining) / batch_size)

if (n_batches > 0) {
  cat(sprintf("Querying BIEN in %d batches of up to %d species...\n", n_batches, batch_size))
}

new_results <- vector("list", n_batches)

for (i in seq_len(n_batches)) {
  start_idx <- (i - 1) * batch_size + 1
  end_idx   <- min(i * batch_size, length(remaining))
  batch     <- remaining[start_idx:end_idx]

  cat(sprintf("  Batch %d/%d: querying %d species...", i, n_batches, length(batch)))

  result <- tryCatch({
    BIEN::BIEN_occurrence_records_per_species(batch)
  }, error = function(e) {
    cat(sprintf(" ERROR: %s\n", conditionMessage(e)))
    NULL
  })

  if (!is.null(result) && nrow(result) > 0) {
    # Rename column if needed
    if ("species" %in% names(result) && !"scrubbed_species_binomial" %in% names(result)) {
      names(result)[names(result) == "species"] <- "scrubbed_species_binomial"
    }
    result$scrubbed_species_binomial_submitted <- result$scrubbed_species_binomial
    new_results[[i]] <- result
    cat(sprintf(" got %d rows\n", nrow(result)))
  } else {
    # Record all batch species as 0-record entries so they appear in output
    new_results[[i]] <- data.frame(
      scrubbed_species_binomial = batch,
      number_of_records = 0L,
      scrubbed_species_binomial_submitted = batch,
      stringsAsFactors = FALSE
    )
    cat(" (no records returned)\n")
  }

  # Append to checkpoint after each batch
  batch_df <- new_results[[i]]
  if (!is.null(batch_df)) {
    write_header <- !file.exists(checkpoint_csv)
    readr::write_csv(batch_df, checkpoint_csv, append = !write_header)
  }

  if (i < n_batches) Sys.sleep(1)
}

# ── 5. Combine all results ─────────────────────────────────────────────────
all_batched <- if (length(new_results) > 0) {
  dplyr::bind_rows(new_results)
} else {
  NULL
}

# Reload full checkpoint (includes prior runs)
full_checkpoint <- if (file.exists(checkpoint_csv)) {
  readr::read_csv(checkpoint_csv, show_col_types = FALSE)
} else {
  all_batched
}

# For species that BIEN returned (may use scrubbed name ≠ submitted name),
# we need to track which submitted name maps to what result.
# We submitted names in batches and got back scrubbed names; use submitted col.
bien_results <- full_checkpoint %>%
  dplyr::select(
    name_submitted = scrubbed_species_binomial_submitted,
    n_bien_records = number_of_records
  ) %>%
  dplyr::group_by(name_submitted) %>%
  dplyr::summarise(n_bien_records = max(n_bien_records, na.rm = TRUE), .groups = "drop")

# Species not returned at all → 0 records
missing_species <- setdiff(all_species, bien_results$name_submitted)
if (length(missing_species) > 0) {
  bien_results <- dplyr::bind_rows(
    bien_results,
    data.frame(name_submitted = missing_species, n_bien_records = 0L)
  )
}

# ── 6. Per-species output ──────────────────────────────────────────────────
# Build species → paper_ids mapping
species_papers <- binomials %>%
  dplyr::group_by(name_submitted) %>%
  dplyr::summarise(paper_ids = paste(sort(unique(paper_id)), collapse = "; "),
                   .groups = "drop")

per_species <- species_papers %>%
  dplyr::left_join(bien_results, by = "name_submitted") %>%
  dplyr::mutate(n_bien_records = dplyr::coalesce(n_bien_records, 0L)) %>%
  dplyr::arrange(dplyr::desc(n_bien_records), name_submitted)

readr::write_csv(per_species, per_species_csv)
cat(sprintf("\nWrote %s (%d rows)\n", per_species_csv, nrow(per_species)))

# ── 7. Per-paper summary ───────────────────────────────────────────────────
per_paper <- binomials %>%
  dplyr::left_join(bien_results, by = "name_submitted") %>%
  dplyr::mutate(n_bien_records = dplyr::coalesce(n_bien_records, 0L),
                in_bien = n_bien_records > 0) %>%
  dplyr::group_by(paper_id) %>%
  dplyr::summarise(
    n_species_submitted = dplyr::n_distinct(name_submitted),
    n_in_bien           = sum(in_bien),
    n_not_in_bien       = n_species_submitted - n_in_bien,
    pct_in_bien         = round(100 * n_in_bien / n_species_submitted, 1),
    total_bien_records  = sum(n_bien_records, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::arrange(dplyr::desc(pct_in_bien))

readr::write_csv(per_paper, per_paper_csv)
cat(sprintf("Wrote %s (%d rows)\n", per_paper_csv, nrow(per_paper)))

# ── 8. Console summary ────────────────────────────────────────────────────
cat("\n── Per-paper BIEN Overlap Summary ──────────────────────────────────\n")
print(as.data.frame(per_paper), row.names = FALSE)
cat("─────────────────────────────────────────────────────────────────────\n")

total_species <- nrow(per_species)
total_in_bien <- sum(per_species$n_bien_records > 0)
cat(sprintf("\nOverall: %d unique binomials; %d (%.1f%%) found in BIEN.\n",
            total_species, total_in_bien,
            100 * total_in_bien / max(total_species, 1)))
cat("Done.\n")
