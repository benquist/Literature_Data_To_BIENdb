#!/usr/bin/env Rscript

args_all <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_path <- sub(file_arg, "", args_all[grep(file_arg, args_all)][1])
project_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
source(file.path(project_root, "scripts", "utils.R"), local = FALSE)

extract_supplementary_assets <- function(xml_txt) {
  blocks <- regmatches(xml_txt, gregexpr("<supplementary-material[\\s\\S]*?</supplementary-material>", xml_txt, perl = TRUE))[[1]]
  if (!length(blocks)) {
    return(data.frame(
      supplementary_id = character(0),
      supplementary_doi = character(0),
      label = character(0),
      media_href = character(0),
      declared_mimetype = character(0),
      declared_mime_subtype = character(0),
      original_file_url = character(0),
      stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(blocks, function(block) {
    get_one <- function(pattern) {
      m <- regmatches(block, regexpr(pattern, block, perl = TRUE))
      if (!length(m) || identical(m, character(0))) return(NA_character_)
      sub(pattern, "\\1", m, perl = TRUE)
    }
    data.frame(
      supplementary_id = get_one('id="([^"]+)"'),
      supplementary_doi = get_one('<object-id content-type="doi">([^<]+)</object-id>'),
      label = get_one('<label>([^<]+)</label>'),
      media_href = get_one('<media[^>]*xlink:href="([^"]+)"'),
      declared_mimetype = get_one('<media[^>]*mimetype="([^"]+)"'),
      declared_mime_subtype = get_one('<media[^>]*mime-subtype="([^"]+)"'),
      original_file_url = get_one('<uri content-type="original_file">([^<]+)</uri>'),
      stringsAsFactors = FALSE
    )
  })

  out <- do.call(rbind, rows)
  out
}

args <- parse_named_args(commandArgs(trailingOnly = TRUE))
root <- find_project_root()
output_dir <- args$`output-dir` %||% file.path(root, "data", "interim")
paper_id <- args$`paper-id` %||% "jennings_2026"
force <- is_true(args$force)
log_file <- file.path(root, "logs", paste0("discover_", paper_id, ".log"))

ensure_dir(output_dir)
append_log(log_file, "INFO", "Discovery started", paste("paper_id=", paper_id))

papers <- read_papers_config(root)
paper <- papers[papers$paper_id == paper_id, , drop = FALSE]
if (!nrow(paper)) stop("paper_id not found in config: ", paper_id, call. = FALSE)

crossref_url <- paste0("https://api.crossref.org/works/", paper$doi[[1]])
crossref_txt <- curl_fetch_text(crossref_url)
if (is.null(crossref_txt)) {
  append_log(log_file, "WARN", "Crossref lookup failed", crossref_url)
  crossref_txt <- ""
}

landing_html <- curl_fetch_text(paper$landing_url[[1]])
if (is.null(landing_html)) {
  append_log(log_file, "WARN", "Landing page fetch failed", paper$landing_url[[1]])
  landing_html <- ""
}

xml_txt <- curl_fetch_text(paper$xml_url[[1]])
if (is.null(xml_txt)) stop("Unable to fetch article XML: ", paper$xml_url[[1]], call. = FALSE)

assets <- extract_supplementary_assets(xml_txt)
if (!nrow(assets)) append_log(log_file, "WARN", "No supplementary assets detected in XML", "")

if (nrow(assets)) {
  headers <- lapply(assets$original_file_url, curl_head)
  assets$content_type <- vapply(headers, function(h) {
    if (!nrow(h)) return(NA_character_)
    idx <- which(h$key == "content-type")
    if (!length(idx)) NA_character_ else h$value[[idx[[1]]]]
  }, character(1))
  assets$content_disposition <- vapply(headers, function(h) {
    if (!nrow(h)) return(NA_character_)
    idx <- which(h$key == "content-disposition")
    if (!length(idx)) NA_character_ else h$value[[idx[[1]]]]
  }, character(1))
}

assets$paper_id <- rep(paper_id, nrow(assets))
assets$doi <- rep(paper$doi[[1]], nrow(assets))
assets$landing_url <- rep(paper$landing_url[[1]], nrow(assets))
assets$discovered_at_utc <- rep(timestamp_utc(), nrow(assets))

crossref_pdf <- if (grepl('download/pdf', crossref_txt, fixed = TRUE)) sub('.*(https://phytokeys\\.pensoft\\.net/article/184780/download/pdf/[^"\\\\]+).*', '\\1', crossref_txt) else NA_character_
crossref_xml <- if (grepl('download/xml', crossref_txt, fixed = TRUE)) sub('.*(https://phytokeys\\.pensoft\\.net/article/184780/download/xml/[^"\\\\]+).*', '\\1', crossref_txt) else paper$xml_url[[1]]

metadata <- data.frame(
  paper_id = paper_id,
  doi = paper$doi[[1]],
  citation = paper$citation[[1]],
  landing_url = paper$landing_url[[1]],
  xml_url = paper$xml_url[[1]],
  crossref_url = crossref_url,
  crossref_pdf_link_guess = crossref_pdf,
  crossref_xml_link_guess = crossref_xml,
  expected_file_ids = paper$expected_file_ids[[1]],
  discovered_asset_count = nrow(assets),
  landing_contains_supplementary_anchor = grepl("supplementary_materials", landing_html, fixed = TRUE),
  discovery_timestamp_utc = timestamp_utc(),
  stringsAsFactors = FALSE
)

assets_path <- file.path(output_dir, paste0(paper_id, "_assets.csv"))
metadata_path <- file.path(output_dir, paste0(paper_id, "_metadata.csv"))
if (!force && file.exists(assets_path)) append_log(log_file, "INFO", "Overwriting existing assets manifest", assets_path)

write_csv(assets, assets_path)
write_csv(metadata, metadata_path)
append_log(log_file, "INFO", "Discovery complete", paste("assets=", nrow(assets), " output=", assets_path))
message("Discovery complete: ", nrow(assets), " assets written to ", assets_path)
