#!/usr/bin/env Rscript

args_all <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_path <- sub(file_arg, "", args_all[grep(file_arg, args_all)][1])
project_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
source(file.path(project_root, "scripts", "utils.R"), local = FALSE)

args <- parse_named_args(commandArgs(trailingOnly = TRUE))
root <- find_project_root()
interim_dir <- args$`interim-dir` %||% file.path(root, "data", "interim")
raw_dir <- args$`raw-dir` %||% file.path(root, "data", "raw")
paper_id <- args$`paper-id` %||% "jennings_2026"
force <- is_true(args$force)
log_file <- file.path(root, "logs", paste0("download_", paper_id, ".log"))

ensure_dir(raw_dir)
ensure_dir(interim_dir)

assets_path <- file.path(interim_dir, paste0(paper_id, "_assets.csv"))
if (!file.exists(assets_path)) stop("Missing assets manifest: ", assets_path, call. = FALSE)
assets <- utils::read.csv(assets_path, stringsAsFactors = FALSE, check.names = FALSE)
if (!nrow(assets)) stop("No assets found in manifest: ", assets_path, call. = FALSE)

paper_raw_dir <- file.path(raw_dir, paper_id)
ensure_dir(paper_raw_dir)

manifest_rows <- vector("list", nrow(assets))
for (i in seq_len(nrow(assets))) {
  url <- assets$original_file_url[[i]]
  if (is.na(url) || !nzchar(url)) {
    manifest_rows[[i]] <- data.frame(
      paper_id = paper_id,
      supplementary_id = assets$supplementary_id[[i]],
      download_url = url,
      local_path = NA_character_,
      status = "blocked",
      method = "none",
      file_size = NA_real_,
      error_message = "No original_file_url in manifest",
      downloaded_at_utc = timestamp_utc(),
      stringsAsFactors = FALSE
    )
    next
  }

  fallback_name <- basename(url)
  if (!nzchar(fallback_name)) fallback_name <- paste0("asset_", i)

  disposition <- assets$content_disposition[[i]] %||% ""
  inferred_name <- fallback_name
  if (grepl("filename=", disposition, fixed = TRUE)) {
    inferred_name <- sub('.*filename="?([^";]+)"?.*', '\\1', disposition)
  } else if (!is.na(assets$media_href[[i]]) && nzchar(assets$media_href[[i]])) {
    inferred_name <- assets$media_href[[i]]
  }
  inferred_name <- gsub("[^A-Za-z0-9._-]+", "_", inferred_name)
  dest <- file.path(paper_raw_dir, inferred_name)

  if (!force && file.exists(dest) && file.size(dest) > 0) {
    manifest_rows[[i]] <- data.frame(
      paper_id = paper_id,
      supplementary_id = assets$supplementary_id[[i]],
      download_url = url,
      local_path = dest,
      status = "skipped_existing",
      method = "none",
      file_size = as.numeric(file.size(dest)),
      error_message = "",
      downloaded_at_utc = timestamp_utc(),
      stringsAsFactors = FALSE
    )
    next
  }

  result <- curl_download(url, dest)
  if (!result$ok) {
    append_log(log_file, "WARN", "Download blocker", paste(url, result$error))
    manifest_rows[[i]] <- data.frame(
      paper_id = paper_id,
      supplementary_id = assets$supplementary_id[[i]],
      download_url = url,
      local_path = dest,
      status = "blocked",
      method = result$method,
      file_size = NA_real_,
      error_message = result$error,
      downloaded_at_utc = timestamp_utc(),
      stringsAsFactors = FALSE
    )
  } else {
    manifest_rows[[i]] <- data.frame(
      paper_id = paper_id,
      supplementary_id = assets$supplementary_id[[i]],
      download_url = url,
      local_path = dest,
      status = "downloaded",
      method = result$method,
      file_size = as.numeric(file.size(dest)),
      error_message = "",
      downloaded_at_utc = timestamp_utc(),
      stringsAsFactors = FALSE
    )
  }
}

manifest <- do.call(rbind, manifest_rows)
manifest_path <- file.path(interim_dir, paste0(paper_id, "_download_manifest.csv"))
write_csv(manifest, manifest_path)

blocked <- sum(manifest$status == "blocked", na.rm = TRUE)
append_log(log_file, "INFO", "Download complete", paste("blocked=", blocked, " manifest=", manifest_path))
message("Download step complete: ", nrow(manifest), " files tracked; blocked=", blocked)
