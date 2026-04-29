#!/usr/bin/env Rscript

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || !nzchar(as.character(x))) y else x

parse_named_args <- function(args) {
  values <- list()
  if (!length(args)) return(values)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    parts <- strsplit(sub("^--", "", arg), "=", fixed = TRUE)[[1]]
    key <- parts[[1]]
    value <- if (length(parts) > 1L) paste(parts[-1L], collapse = "=") else "TRUE"
    values[[key]] <- value
  }
  values
}

find_project_root <- function() {
  cwd <- getwd()
  if (basename(cwd) == "Literature_Data_To_BIENdb") return(cwd)
  if (basename(cwd) == "scripts" && basename(dirname(cwd)) == "Literature_Data_To_BIENdb") return(dirname(cwd))
  candidate <- file.path(cwd, "Literature_Data_To_BIENdb")
  if (dir.exists(candidate)) return(candidate)
  stop("Cannot locate Literature_Data_To_BIENdb root from: ", cwd, call. = FALSE)
}

ensure_dir <- function(path) {
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
}

timestamp_utc <- function() format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

append_log <- function(log_file, level, message, context = "") {
  ensure_dir(dirname(log_file))
  line <- paste(timestamp_utc(), level, message, context, sep = " | ")
  cat(line, "\n", file = log_file, append = TRUE)
}

read_papers_config <- function(root) {
  cfg <- file.path(root, "config", "papers.csv")
  if (!file.exists(cfg)) stop("Missing config file: ", cfg, call. = FALSE)
  utils::read.csv(cfg, stringsAsFactors = FALSE, check.names = FALSE)
}

normalize_name <- function(x) {
  out <- tolower(trimws(as.character(x)))
  out <- gsub("[^a-z0-9]+", "_", out)
  out <- gsub("^_+|_+$", "", out)
  out
}

is_true <- function(x) {
  identical(toupper(as.character(x %||% "FALSE")), "TRUE")
}

safe_numeric <- function(x) suppressWarnings(as.numeric(x))

write_csv <- function(x, path) {
  ensure_dir(dirname(path))
  utils::write.csv(x, path, row.names = FALSE, na = "")
}

curl_fetch_text <- function(url, timeout_sec = 60L) {
  out <- tryCatch(
    system2("curl", c("-L", "-sS", "--max-time", as.character(timeout_sec), url), stdout = TRUE, stderr = TRUE),
    error = function(e) character(0)
  )
  if (!length(out)) return(NULL)
  paste(out, collapse = "\n")
}

curl_head <- function(url, timeout_sec = 30L) {
  out <- tryCatch(
    system2("curl", c("-L", "-sS", "-I", "--max-time", as.character(timeout_sec), url), stdout = TRUE, stderr = TRUE),
    error = function(e) character(0)
  )
  if (!length(out)) return(data.frame(stringsAsFactors = FALSE))
  lines <- trimws(out)
  lines <- lines[nzchar(lines)]
  kv <- strsplit(lines[grepl(":", lines, fixed = TRUE)], ":", fixed = TRUE)
  if (!length(kv)) return(data.frame(stringsAsFactors = FALSE))
  data.frame(
    key = tolower(trimws(vapply(kv, `[`, character(1), 1))),
    value = trimws(vapply(kv, function(z) paste(z[-1], collapse = ":"), character(1))),
    stringsAsFactors = FALSE
  )
}

curl_download <- function(url, dest, timeout_sec = 180L) {
  ensure_dir(dirname(dest))
  status <- tryCatch(
    {
      utils::download.file(url, destfile = dest, mode = "wb", quiet = TRUE, method = "libcurl", timeout = timeout_sec)
      0L
    },
    error = function(e) 1L
  )
  if (identical(status, 0L) && file.exists(dest) && file.size(dest) > 0) {
    return(list(ok = TRUE, method = "download.file", error = ""))
  }

  cmd_status <- tryCatch(
    system2("curl", c("-L", "-sS", "--max-time", as.character(timeout_sec), "-o", dest, url)),
    error = function(e) 1L
  )

  if (identical(cmd_status, 0L) && file.exists(dest) && file.size(dest) > 0) {
    return(list(ok = TRUE, method = "curl", error = ""))
  }

  list(ok = FALSE, method = "failed", error = paste("Download failed for", url))
}
