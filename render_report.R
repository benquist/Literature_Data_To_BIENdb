#!/usr/bin/env Rscript
library(rmarkdown)
library(here)

rmd_file <- file.path(here::here(), 'reports/literature_data_overview.Rmd')
output_file <- file.path(here::here(), 'reports/literature_data_overview.html')

cat('Rendering RMarkdown report...\n')
cat('Input:', rmd_file, '\n')
cat('Output:', output_file, '\n')

render(
  rmd_file,
  output_file = output_file,
  quiet = FALSE
)

cat('\nSUCCESS!\n')
cat('Report generated:', output_file, '\n')
