options(stringsAsFactors = FALSE)

library(readr)
library(dplyr)
library(stringr)
library(tidyr)
library(ggplot2)
library(scales)

project_root <- normalizePath(file.path(getwd(), "..", ".."), mustWork = TRUE)
if (basename(getwd()) == "mma_philanthrophy") {
  project_root <- getwd()
}

path_raw <- file.path(project_root, "raw_data")
path_outputs <- file.path(project_root, "processed_data")
path_intermediate <- file.path(path_outputs, "intermediate")
path_final <- file.path(path_outputs, "final")
path_figures <- file.path(path_outputs, "figures")
path_pipeline <- file.path(project_root, "src", "pipeline")
path_taxonomy <- file.path(path_pipeline, "taxonomies")
path_templates <- file.path(path_pipeline, "templates")

dir.create(path_outputs, showWarnings = FALSE, recursive = TRUE)
dir.create(path_intermediate, showWarnings = FALSE, recursive = TRUE)
dir.create(path_final, showWarnings = FALSE, recursive = TRUE)
dir.create(path_figures, showWarnings = FALSE, recursive = TRUE)
dir.create(path_taxonomy, showWarnings = FALSE, recursive = TRUE)
dir.create(path_templates, showWarnings = FALSE, recursive = TRUE)

file_predictions <- file.path(path_raw, "predictions.csv")
file_mbf <- file.path(path_raw, "irs_mbf.csv")
file_fips <- file.path(path_raw, "irs_to_fips.csv")
file_urls <- file.path(path_raw, "irs_urls.csv")
file_url_web <- file.path(path_raw, "irs_urls_websites.csv")

ruca_url <- "https://www.ers.usda.gov/media/5444/2020-rural-urban-commuting-area-codes-zip-codes.csv?v=97137"
file_ruca_cache <- file.path(path_intermediate, "usda_ruca_zip_2020.csv")

file_foundation_universe <- file.path(path_intermediate, "foundation_universe.csv.gz")
file_focus_classified <- file.path(path_intermediate, "foundation_focus_classified.csv.gz")
file_construct_taxonomy <- file.path(path_taxonomy, "democracy_state_capacity_keywords.csv")
file_construct_scores <- file.path(path_intermediate, "foundation_construct_scores.csv.gz")
file_manual_label_template <- file.path(path_templates, "manual_labels_democracy_state_capacity_template.csv")
file_taie_taxonomy <- file.path(path_taxonomy, "tech_ai_innovation_entrepreneurship_keywords.csv")
file_taie_scores <- file.path(path_intermediate, "foundation_taie_scores.csv.gz")
file_issue_taxonomy <- file.path(path_taxonomy, "issue_focus_keywords.csv")
file_web_texts <- file.path(path_intermediate, "foundation_web_texts.csv.gz")
file_web_texts_summary <- file.path(path_final, "foundation_web_texts_summary.csv")
file_web_text_failures <- file.path(path_final, "foundation_web_text_failures.csv")
