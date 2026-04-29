#!/usr/bin/env Rscript

args_all <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_path <- sub(file_arg, "", args_all[grep(file_arg, args_all)][1])
project_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
source(file.path(project_root, "scripts", "utils.R"), local = FALSE)

args <- parse_named_args(commandArgs(trailingOnly = TRUE))
root <- find_project_root()
paper_id <- args$`paper-id` %||% "jennings_2026"
force <- is_true(args$force)

run_log <- file.path(root, "logs", paste0("pipeline_", paper_id, "_", format(Sys.time(), "%Y%m%dT%H%M%S", tz = "UTC"), ".log"))
append_log(run_log, "INFO", "Pipeline start", paste("paper_id=", paper_id, " force=", force))

run_step <- function(script_name, extra_args = character(0)) {
  script_path <- file.path(root, "scripts", script_name)
  cmd <- c(script_path, paste0("--paper-id=", paper_id), extra_args)
  out <- tryCatch(
    system2("Rscript", cmd, stdout = TRUE, stderr = TRUE),
    error = function(e) c("ERROR", conditionMessage(e))
  )
  status <- attr(out, "status") %||% 0L
  cat(paste0("\n===== ", script_name, " =====\n"), file = run_log, append = TRUE)
  if (length(out)) cat(paste(out, collapse = "\n"), "\n", file = run_log, append = TRUE)
  cat(paste0("STATUS: ", status, "\n"), file = run_log, append = TRUE)
  if (!identical(as.integer(status), 0L)) {
    stop("Step failed: ", script_name, " (status ", status, ")", call. = FALSE)
  }
  invisible(out)
}

force_arg <- if (force) "--force=TRUE" else "--force=FALSE"

run_step("01_discover_paper_assets.R", c(force_arg))
run_step("02_download_sources.R", c(force_arg))
run_step("03_normalize_to_dwc.R")
run_step("04_build_bien_staging.R")

append_log(run_log, "INFO", "Pipeline end", "Completed all steps")
message("Pipeline completed. Log: ", run_log)
