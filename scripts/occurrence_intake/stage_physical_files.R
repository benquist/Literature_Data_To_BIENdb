#!/usr/bin/env Rscript
# stage_physical_files.R
# Staging helper for pending manually-supplied physical files.
# Run from the Literature_Data_To_BIENdb root to inspect column structure before mapping.
#
# Expected files — place these in data/manual_ingestion/ before running:
# 1. "alltraits with phynames.csv" — Spasojevic 2016 Ozark study, Jesse Miller supplied
#    (may duplicate Dryad doi:10.5061/dryad.rr4pm — check column overlap before ingesting)
# 2. "oztrait data JoE.csv" — Spasojevic 2016 Ozark study, Jesse Miller supplied
# 3. "ozark trait metadata2.xlsx" — Metadata companion for Spasojevic 2016 Ozark study
# 4. "Biodiversity and Ecological long-term plots in Southern Patagonia (BEPSP)_Complete_(Pablo Peri).xlsx"
#    — Peri et al. 2016, J. Nature Conservation, doi:10.1016/j.jnc.2016.09.003

library(data.table)

INTAKE_DIR <- file.path("data", "manual_ingestion")

stage_spasojevic_alltraits <- function() {
  path <- file.path(INTAKE_DIR, "alltraits with phynames.csv")
  if (!file.exists(path)) {
    message("[MISSING] Place 'alltraits with phynames.csv' in: ", INTAKE_DIR)
    return(invisible(NULL))
  }
  message("[FOUND] Reading: ", path)
  d <- fread(path)
  message("Dimensions: ", nrow(d), " rows x ", ncol(d), " cols")
  message("Column names:")
  print(names(d))
  message("Head (5 rows):")
  print(head(d, 5))
  invisible(d)
}

stage_spasojevic_oztrait <- function() {
  path <- file.path(INTAKE_DIR, "oztrait data JoE.csv")
  if (!file.exists(path)) {
    message("[MISSING] Place 'oztrait data JoE.csv' in: ", INTAKE_DIR)
    return(invisible(NULL))
  }
  message("[FOUND] Reading: ", path)
  d <- fread(path)
  message("Dimensions: ", nrow(d), " rows x ", ncol(d), " cols")
  message("Column names:")
  print(names(d))
  message("Head (5 rows):")
  print(head(d, 5))
  invisible(d)
}

stage_spasojevic_metadata <- function() {
  path <- file.path(INTAKE_DIR, "ozark trait metadata2.xlsx")
  if (!file.exists(path)) {
    message("[MISSING] Place 'ozark trait metadata2.xlsx' in: ", INTAKE_DIR)
    return(invisible(NULL))
  }
  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    stop("Package 'openxlsx' is required. Install with: install.packages('openxlsx')", call. = FALSE)
  }
  message("[FOUND] Reading: ", path)
  d <- openxlsx::read.xlsx(path)
  message("Dimensions: ", nrow(d), " rows x ", ncol(d), " cols")
  message("Column names:")
  print(names(d))
  message("Head (5 rows):")
  print(head(d, 5))
  invisible(d)
}

stage_peri_bepsp <- function() {
  fname <- "Biodiversity and Ecological long-term plots in Southern Patagonia (BEPSP)_Complete_(Pablo Peri).xlsx"
  path  <- file.path(INTAKE_DIR, fname)
  if (!file.exists(path)) {
    message("[MISSING] Place '", fname, "' in: ", INTAKE_DIR)
    return(invisible(NULL))
  }
  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    stop("Package 'openxlsx' is required. Install with: install.packages('openxlsx')", call. = FALSE)
  }
  message("[FOUND] Reading: ", path)
  d <- openxlsx::read.xlsx(path)
  message("Dimensions: ", nrow(d), " rows x ", ncol(d), " cols")
  message("Column names:")
  print(names(d))
  message("Head (5 rows):")
  print(head(d, 5))
  invisible(d)
}

# --- Runner ---
stage_spasojevic_alltraits()
stage_spasojevic_oztrait()
stage_spasojevic_metadata()
stage_peri_bepsp()
