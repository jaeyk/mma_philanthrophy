normalize_url <- function(x) {
  x %>%
    str_trim() %>%
    str_replace("^https?://", "") %>%
    str_replace("^www\\.", "") %>%
    str_replace("/+$", "") %>%
    str_to_lower()
}

extract_domain <- function(x) {
  x %>%
    str_trim() %>%
    str_replace("^https?://", "") %>%
    str_replace("^www\\.", "") %>%
    str_extract("^[^/]+") %>%
    str_to_lower()
}

safe_numeric <- function(x) suppressWarnings(as.numeric(x))

gini <- function(x) {
  x <- as.numeric(x)
  x <- x[!is.na(x) & x >= 0]
  n <- length(x)
  if (n == 0) return(NA_real_)
  s <- sum(x)
  if (s == 0) return(NA_real_)
  x <- sort(x)
  i <- seq_len(n)
  (2 * sum(i * x)) / (n * s) - (n + 1) / n
}

top_share <- function(x, p = 0.01) {
  x <- as.numeric(x)
  x <- x[!is.na(x) & x > 0]
  if (length(x) == 0) return(NA_real_)
  cutoff <- as.numeric(quantile(x, 1 - p, na.rm = TRUE))
  sum(x[x >= cutoff], na.rm = TRUE) / sum(x, na.rm = TRUE)
}

entropy <- function(x) {
  x <- as.numeric(x)
  x[is.na(x)] <- 0
  s <- sum(x)
  if (s <= 0) return(NA_real_)
  p <- x / s
  p <- p[p > 0]
  -sum(p * log(p))
}

fetch_ruca <- function(file_ruca_cache, ruca_url) {
  if (!file.exists(file_ruca_cache)) {
    message("Downloading RUCA ZIP file from USDA ERS...")
    tmp <- tempfile(fileext = ".csv")
    suppressWarnings(download.file(ruca_url, tmp, mode = "wb", quiet = TRUE))
    file.copy(tmp, file_ruca_cache, overwrite = TRUE)
  }

  readr::read_csv(file_ruca_cache, show_col_types = FALSE) %>%
    transmute(
      zip5 = stringr::str_sub(stringr::str_pad(as.character(ZIPCode), 5, side = "left", pad = "0"), 1, 5),
      ruca_primary = as.numeric(PrimaryRUCA),
      ruca_secondary = as.numeric(SecondaryRUCA),
      ruca_category = dplyr::case_when(
        ruca_primary >= 1 & ruca_primary < 4 ~ "Metropolitan",
        ruca_primary >= 4 & ruca_primary < 7 ~ "Micropolitan",
        ruca_primary >= 7 & ruca_primary < 10 ~ "Small town",
        ruca_primary == 10 ~ "Rural",
        TRUE ~ NA_character_
      )
    ) %>%
    distinct(zip5, .keep_all = TRUE)
}

build_signal_text <- function(fdn, file_web_texts) {
  base_text <- fdn %>%
    transmute(
      ein,
      base_text = str_to_lower(str_c(
        coalesce(name, ""), " ",
        coalesce(taxpayer_name, ""), " ",
        coalesce(candidate_url, ""), " ",
        coalesce(domain, ""), " ",
        coalesce(ntee_cd, "")
      ))
    ) %>%
    distinct(ein, .keep_all = TRUE)

  if (file.exists(file_web_texts)) {
    web_agg <- readr::read_csv(file_web_texts, show_col_types = FALSE) %>%
      filter(!is.na(text_clean), text_clean != "") %>%
      group_by(ein) %>%
      summarize(web_text = str_to_lower(str_c(text_clean, collapse = " ")), .groups = "drop")

    out <- base_text %>%
      left_join(web_agg, by = "ein") %>%
      mutate(
        signal_text = str_squish(str_c(base_text, " ", coalesce(web_text, "")))
      ) %>%
      select(ein, signal_text)
  } else {
    out <- base_text %>%
      mutate(signal_text = str_squish(base_text)) %>%
      select(ein, signal_text)
  }

  out
}
