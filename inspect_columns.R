#!/usr/bin/env Rscript
files <- list.files('data/processed', pattern = '_bien_staging.csv$', full.names = TRUE)

cat("Column summary across all papers:\n\n")

all_cols <- list()
for (f in files) {
  paper_id <- gsub("_bien_staging\\.csv$", "", basename(f))
  d <- read.csv(f, nrows = 1, check.names = FALSE, stringsAsFactors = FALSE)
  cols <- names(d)
  all_cols[[paper_id]] <- cols
  cat(paper_id, ":\n  Total cols:", length(cols), "\n")
  cat("  First 20:", paste(head(cols, 20), collapse = ", "), "\n\n")
}

# Find common columns
if (length(all_cols) > 0) {
  common <- Reduce(intersect, all_cols)
  cat("\n=== Common columns across ALL papers ===\n")
  cat(paste(common, collapse = "\n"), "\n\n")
}

# Show what eventDate-like columns exist
cat("=== Date-related columns ===\n")
for (pid in names(all_cols)) {
  date_cols <- all_cols[[pid]][grepl("date|Date", all_cols[[pid]], ignore.case = TRUE)]
  if (length(date_cols) > 0) {
    cat(pid, ":", paste(date_cols, collapse = ", "), "\n")
  }
}
