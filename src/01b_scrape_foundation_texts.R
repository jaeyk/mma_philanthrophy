source(file.path("src", "00_config.R"))
source(file.path("src", "utils.R"))

message("[01b] Scraping foundation website text for reusable classification corpus...")

if (!requireNamespace("httr2", quietly = TRUE) ||
    !requireNamespace("rvest", quietly = TRUE) ||
    !requireNamespace("xml2", quietly = TRUE)) {
  stop("Please install required packages: httr2, rvest, xml2")
}

domain_reco <- tibble(domain = character(), recommendation = character())
if (file.exists(file_web_failure_domain_recommendations)) {
  domain_reco <- read_csv(file_web_failure_domain_recommendations, show_col_types = FALSE) %>%
    transmute(domain = str_to_lower(domain), recommendation = str_to_lower(recommendation))
}

USE_DIAGNOSTIC_DOMAIN_FILTERS <- as.integer(Sys.getenv("USE_DIAGNOSTIC_DOMAIN_FILTERS", unset = "0"))
if (is.na(USE_DIAGNOSTIC_DOMAIN_FILTERS)) {
  USE_DIAGNOSTIC_DOMAIN_FILTERS <- 0L
}

fdn <- read_csv(file_foundation_universe, show_col_types = FALSE) %>%
  filter(!is.na(candidate_url), candidate_url != "") %>%
  mutate(
    seed_url = candidate_url,
    seed_domain = extract_domain(seed_url),
    foundation_name = coalesce(name, taxpayer_name, "")
  ) %>%
  distinct(ein, .keep_all = TRUE)

URL_FILTER_MODE <- str_to_lower(str_trim(Sys.getenv("URL_FILTER_MODE", unset = "candidate")))
if (URL_FILTER_MODE == "quality_keep") {
  fdn <- fdn %>% filter(url_quality_keep %in% TRUE)
} else if (URL_FILTER_MODE != "candidate") {
  warning(sprintf(
    "[01b] Unknown URL_FILTER_MODE='%s'; falling back to 'candidate'. Valid: candidate, quality_keep",
    URL_FILTER_MODE
  ))
  URL_FILTER_MODE <- "candidate"
}
message(sprintf("[01b] URL filter mode: %s | foundations selected: %d", URL_FILTER_MODE, nrow(fdn)))

max_orgs_raw <- str_trim(Sys.getenv("MAX_ORGS", unset = "ALL"))
MAX_ORGS <- if (max_orgs_raw == "" || str_to_upper(max_orgs_raw) == "ALL") {
  Inf
} else {
  suppressWarnings(as.integer(max_orgs_raw))
}
MAX_PAGES_PER_SITE <- as.integer(Sys.getenv("MAX_PAGES_PER_SITE", unset = "4"))
REQUEST_TIMEOUT <- as.integer(Sys.getenv("REQUEST_TIMEOUT_SECONDS", unset = "20"))
CRAWL_DELAY <- as.numeric(Sys.getenv("CRAWL_DELAY_SECONDS", unset = "0.4"))
MAX_RETRIES <- as.integer(Sys.getenv("MAX_RETRIES", unset = "2"))
SCRAPER_VERBOSE <- as.integer(Sys.getenv("SCRAPER_VERBOSE", unset = "1"))
MIN_TEXT_CHARS_STRONG_SUCCESS <- as.integer(Sys.getenv("MIN_TEXT_CHARS_STRONG_SUCCESS", unset = "600"))
CONTINUE_IF_THIN_SUCCESS <- as.integer(Sys.getenv("CONTINUE_IF_THIN_SUCCESS", unset = "1"))
SCRAPER_WORKERS <- as.integer(Sys.getenv("SCRAPER_WORKERS", unset = "1"))
SCRAPER_RESUME <- as.integer(Sys.getenv("SCRAPER_RESUME", unset = "1"))
SCRAPER_CHECKPOINT_EVERY <- as.integer(Sys.getenv("SCRAPER_CHECKPOINT_EVERY", unset = "100"))
SCRAPER_CHECKPOINT_FILE <- Sys.getenv(
  "SCRAPER_CHECKPOINT_FILE",
  unset = file.path(path_intermediate, "foundation_web_texts_checkpoint.csv")
)

BROWSER_FALLBACK_ENABLED <- as.integer(Sys.getenv("BROWSER_FALLBACK_ENABLED", unset = "0"))
BROWSER_FALLBACK_TIMEOUT_SECONDS <- as.integer(Sys.getenv("BROWSER_FALLBACK_TIMEOUT_SECONDS", unset = "25"))
BROWSER_FALLBACK_WAIT_MS <- as.integer(Sys.getenv("BROWSER_FALLBACK_WAIT_MS", unset = "700"))
BROWSER_FALLBACK_MIN_TEXT_CHARS <- as.integer(Sys.getenv("BROWSER_FALLBACK_MIN_TEXT_CHARS", unset = "350"))
BROWSER_FALLBACK_SCRIPT <- Sys.getenv("BROWSER_FALLBACK_SCRIPT", unset = file.path("src", "playwright_fetch.mjs"))

if (is.finite(MAX_ORGS) && !is.na(MAX_ORGS) && MAX_ORGS > 0 && nrow(fdn) > MAX_ORGS) {
  fdn <- fdn %>% slice_head(n = MAX_ORGS)
}

if (is.na(SCRAPER_CHECKPOINT_EVERY) || SCRAPER_CHECKPOINT_EVERY < 1) {
  SCRAPER_CHECKPOINT_EVERY <- 100L
}

normalize_web_rows <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(tibble(
      ein = character(),
      foundation_name = character(),
      source_url = character(),
      final_url = character(),
      status_code = integer(),
      title = character(),
      text_clean = character(),
      scraped_ok = logical(),
      error_type = character(),
      error_message = character(),
      attempts = integer(),
      browser_fallback_used = logical(),
      scraped_at = character()
    ))
  }

  if (!"foundation_name" %in% names(df)) df$foundation_name <- NA_character_
  if (!"source_url" %in% names(df)) df$source_url <- NA_character_
  if (!"final_url" %in% names(df)) df$final_url <- NA_character_
  if (!"status_code" %in% names(df)) df$status_code <- NA_integer_
  if (!"title" %in% names(df)) df$title <- ""
  if (!"text_clean" %in% names(df)) df$text_clean <- ""
  if (!"scraped_ok" %in% names(df)) df$scraped_ok <- FALSE
  if (!"error_type" %in% names(df)) df$error_type <- ""
  if (!"error_message" %in% names(df)) df$error_message <- ""
  if (!"attempts" %in% names(df)) df$attempts <- NA_integer_
  if (!"browser_fallback_used" %in% names(df)) df$browser_fallback_used <- FALSE
  if (!"scraped_at" %in% names(df)) df$scraped_at <- NA_character_

  df %>%
    transmute(
      ein = as.character(ein),
      foundation_name = as.character(foundation_name),
      source_url = as.character(source_url),
      final_url = as.character(final_url),
      status_code = suppressWarnings(as.integer(status_code)),
      title = as.character(title),
      text_clean = as.character(text_clean),
      scraped_ok = as.logical(scraped_ok),
      error_type = as.character(error_type),
      error_message = as.character(error_message),
      attempts = suppressWarnings(as.integer(attempts)),
      browser_fallback_used = as.logical(browser_fallback_used),
      scraped_at = as.character(scraped_at)
    )
}

existing_records <- tibble()
if (SCRAPER_RESUME != 1 && file.exists(SCRAPER_CHECKPOINT_FILE)) {
  unlink(SCRAPER_CHECKPOINT_FILE)
}

if (SCRAPER_RESUME == 1 && file.exists(SCRAPER_CHECKPOINT_FILE)) {
  existing_records <- read_csv(SCRAPER_CHECKPOINT_FILE, show_col_types = FALSE) %>%
    normalize_web_rows()
  completed_eins <- existing_records %>% distinct(ein) %>% pull(ein)
  if (length(completed_eins) > 0) {
    before_n <- nrow(fdn)
    fdn <- fdn %>% filter(!ein %in% completed_eins)
    resumed_n <- before_n - nrow(fdn)
    message(sprintf(
      "[01b] Resume enabled: loaded %d checkpoint rows (%d EINs already processed).",
      nrow(existing_records), resumed_n
    ))
  }
}

extract_text_from_html <- function(html_raw) {
  doc <- xml2::read_html(html_raw)
  title <- rvest::html_text2(rvest::html_element(doc, "title"))
  body_txt <- rvest::html_text2(rvest::html_element(doc, "body"))
  body_txt <- str_squish(body_txt)
  body_txt <- str_replace_all(body_txt, "\\s+", " ")
  list(title = coalesce(title, ""), text = body_txt)
}

is_likely_js_rendered <- function(html_raw, text_clean) {
  if (is.null(html_raw) || is.na(html_raw) || !nzchar(html_raw)) return(FALSE)
  html_low <- str_to_lower(html_raw)
  script_count <- str_count(html_low, "<script")
  framework_hint <- str_detect(html_low, "id=\\\"(root|app|__next)\\\"") ||
    str_detect(html_low, "window\\.__") ||
    str_detect(html_low, "webpack")
  thin_text <- nchar(coalesce(text_clean, "")) < BROWSER_FALLBACK_MIN_TEXT_CHARS
  (script_count >= 12 && thin_text) || (script_count >= 4 && framework_hint && thin_text)
}

safe_fetch <- function(url) {
  out <- list(
    status_code = NA_integer_, final_url = url, title = "", text_clean = "",
    ok = FALSE, error_type = "", error_message = "", attempts = 0L,
    likely_js = FALSE
  )
  resp <- tryCatch({
    httr2::request(url) %>%
      httr2::req_user_agent("foundation-focus-pipeline/0.1") %>%
      httr2::req_timeout(REQUEST_TIMEOUT) %>%
      httr2::req_perform()
  }, error = function(e) e)

  if (inherits(resp, "error")) {
    out$error_type <- "request_error"
    out$error_message <- conditionMessage(resp)
    return(out)
  }

  out$status_code <- httr2::resp_status(resp)
  out$final_url <- httr2::resp_url(resp)
  ctype <- httr2::resp_header(resp, "content-type")
  if (is.null(ctype)) ctype <- ""
  if (is.na(ctype)) ctype <- ""
  ctype <- tolower(ctype)

  if (!(grepl("text/html", ctype, fixed = TRUE) || ctype == "")) {
    out$error_type <- "non_html"
    out$error_message <- ctype
    return(out)
  }

  html_raw <- tryCatch(httr2::resp_body_string(resp), error = function(e) NA_character_)
  if (is.null(html_raw) || is.na(html_raw) || !nzchar(html_raw)) {
    out$error_type <- "empty_body"
    out$error_message <- "No body extracted"
    return(out)
  }

  parsed <- tryCatch(extract_text_from_html(html_raw), error = function(e) list(title = "", text = ""))
  out$title <- coalesce(as.character(parsed$title), "")
  out$text_clean <- coalesce(as.character(parsed$text), "")
  out$ok <- nzchar(out$text_clean)
  out$likely_js <- is_likely_js_rendered(html_raw, out$text_clean)
  if (!out$ok) {
    out$error_type <- "empty_text"
    out$error_message <- "Body parsed but no usable text"
  }
  out
}

collect_candidate_links <- function(seed_url, domain, html_raw = NULL) {
  base_paths <- c("/about", "/about-us", "/mission", "/programs", "/grants", "/what-we-fund")
  base_urls <- unique(c(seed_url, str_c("https://", domain, base_paths), str_c("http://", domain, base_paths)))
  base_urls <- base_urls[!is.na(base_urls) & base_urls != ""]
  unique(base_urls)
}

order_links_by_protocol <- function(links, prefer_https = FALSE) {
  if (!prefer_https) return(links)
  https_idx <- str_starts(str_to_lower(links), "https://")
  c(links[https_idx], links[!https_idx])
}

is_root_url <- function(u) {
  u2 <- str_replace(u, "^https?://", "")
  slash_pos <- regexpr("/", u2, fixed = TRUE)[1]
  if (slash_pos == -1) return(TRUE)
  path <- substr(u2, slash_pos, nchar(u2))
  path == "/" || path == ""
}

probe_domain_reachability <- function(domain) {
  if (is.na(domain) || domain == "") return(FALSE)
  probe_urls <- unique(c(
    str_c("https://", domain, "/"),
    str_c("http://", domain, "/"),
    str_c("https://www.", domain, "/"),
    str_c("http://www.", domain, "/")
  ))
  for (pu in probe_urls) {
    res <- safe_fetch(pu)
    if (isTRUE(res$ok)) return(TRUE)
    if (!is.na(res$status_code) && res$status_code >= 200 && res$status_code < 500) return(TRUE)
    if (identical(res$error_type, "non_html")) return(TRUE)
  }
  FALSE
}

build_seed_pool <- function(row) {
  get_cell <- function(df_row, col_name) {
    if (!col_name %in% names(df_row)) return(NA_character_)
    as.character(df_row[[col_name]][1])
  }
  split_links <- function(x) {
    if (is.null(x) || is.na(x) || x == "") return(character(0))
    parts <- unlist(str_split(x, ",", simplify = FALSE))
    parts <- str_squish(parts)
    parts[parts != ""]
  }
  pool <- c(
    get_cell(row, "candidate_url"),
    get_cell(row, "preferred_link"),
    get_cell(row, "first_deep_link"),
    get_cell(row, "irs_url"),
    get_cell(row, "first_link"),
    split_links(get_cell(row, "all_deep_links"))
  )
  pool <- pool[!is.na(pool) & pool != ""]
  if (length(pool) == 0) return(character(0))
  norm <- normalize_url(pool)
  pool[!duplicated(norm)]
}

browser_fallback_available <- FALSE
if (BROWSER_FALLBACK_ENABLED == 1) {
  if (!file.exists(BROWSER_FALLBACK_SCRIPT)) {
    warning(sprintf("[01b] Browser fallback disabled: script not found at %s", BROWSER_FALLBACK_SCRIPT))
  } else {
    node_ok <- nzchar(Sys.which("node"))
    if (!node_ok) {
      warning("[01b] Browser fallback disabled: node binary not found on PATH")
    } else {
      browser_fallback_available <- TRUE
    }
  }
}

safe_browser_fetch <- function(url) {
  out <- list(
    status_code = NA_integer_, final_url = url, title = "", text_clean = "",
    ok = FALSE, error_type = "browser_unavailable", error_message = "Browser fallback unavailable"
  )
  if (!browser_fallback_available) return(out)

  res <- tryCatch(
    system2(
      command = "node",
      args = c(BROWSER_FALLBACK_SCRIPT, url, as.character(BROWSER_FALLBACK_TIMEOUT_SECONDS), as.character(BROWSER_FALLBACK_WAIT_MS)),
      stdout = TRUE,
      stderr = TRUE
    ),
    error = function(e) e
  )

  if (inherits(res, "error")) {
    out$error_type <- "browser_exec_error"
    out$error_message <- conditionMessage(res)
    return(out)
  }

  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    out$error_type <- "browser_missing_jsonlite"
    out$error_message <- "Install R package jsonlite for browser fallback parsing"
    return(out)
  }

  raw <- paste(res, collapse = "\n")
  lines <- unlist(str_split(raw, "\n"))
  json_lines <- lines[str_starts(str_trim(lines), "{")]
  if (length(json_lines) == 0) {
    out$error_type <- "browser_no_json_output"
    out$error_message <- str_sub(raw, 1, 500)
    return(out)
  }

  parsed <- tryCatch(jsonlite::fromJSON(json_lines[length(json_lines)]), error = function(e) NULL)
  if (is.null(parsed)) {
    out$error_type <- "browser_json_parse_error"
    out$error_message <- str_sub(json_lines[length(json_lines)], 1, 500)
    return(out)
  }

  out$status_code <- suppressWarnings(as.integer(parsed$status_code %||% NA_integer_))
  out$final_url <- as.character(parsed$final_url %||% url)
  out$title <- as.character(parsed$title %||% "")
  out$text_clean <- as.character(parsed$text_clean %||% "")
  out$ok <- isTRUE(parsed$ok) && nzchar(out$text_clean)
  out$error_type <- as.character(parsed$error_type %||% "")
  out$error_message <- as.character(parsed$error_message %||% "")
  out
}

`%||%` <- function(a, b) if (!is.null(a) && !is.na(a)) a else b

should_try_browser_fallback <- function(fetched) {
  if (!browser_fallback_available) return(FALSE)
  text_chars <- nchar(coalesce(fetched$text_clean, ""))
  weak_success <- isTRUE(fetched$ok) && text_chars < BROWSER_FALLBACK_MIN_TEXT_CHARS
  recoverable_failure <- !isTRUE(fetched$ok) && fetched$error_type %in% c("request_error", "empty_body", "empty_text")
  isTRUE(fetched$likely_js) || weak_success || recoverable_failure
}

format_hms <- function(seconds) {
  if (is.na(seconds) || !is.finite(seconds) || seconds < 0) return("--:--:--")
  s <- as.integer(round(seconds))
  h <- s %/% 3600
  m <- (s %% 3600) %/% 60
  sec <- s %% 60
  sprintf("%02d:%02d:%02d", h, m, sec)
}

write_checkpoint_rows <- function(rows_df) {
  if (is.null(rows_df) || nrow(rows_df) == 0) return(invisible(NULL))
  readr::write_csv(
    normalize_web_rows(rows_df),
    SCRAPER_CHECKPOINT_FILE,
    append = file.exists(SCRAPER_CHECKPOINT_FILE)
  )
  invisible(NULL)
}

process_foundation_row <- function(row_i, idx, n_total, skip_domains, prefer_domains) {
  ein_i <- row_i$ein
  seed_i <- row_i$seed_url
  name_i <- str_squish(coalesce(row_i$foundation_name, ""))
  if (is.na(seed_i) || seed_i == "") return(list())

  if (SCRAPER_VERBOSE == 1) {
    message(sprintf("[01b] [%d/%d] EIN %s | %s", idx, n_total, ein_i, name_i))
  }

  seed_pool <- build_seed_pool(row_i)
  if (length(seed_pool) == 0) return(list())

  seed_pool <- seed_pool[seq_len(min(length(seed_pool), 3))]
  links <- character(0)
  doms <- character(0)
  for (s in seed_pool) {
    d <- extract_domain(s)
    if (!is.na(d) && d != "") doms <- c(doms, d)
    links <- c(links, collect_candidate_links(s, d))
  }
  doms <- unique(str_to_lower(doms))

  should_skip_domain <- any(doms %in% skip_domains)
  prefer_https <- any(doms %in% prefer_domains)

  if (should_skip_domain) {
    if (SCRAPER_VERBOSE == 1) {
      message("  -> skipped entire domain by diagnostics recommendation (skip_domain)")
    }
    return(list(tibble(
      ein = ein_i,
      foundation_name = name_i,
      source_url = seed_i,
      final_url = seed_i,
      status_code = NA_integer_,
      title = "",
      text_clean = "",
      scraped_ok = FALSE,
      error_type = "diagnostic_skip_domain",
      error_message = "Domain marked skip_domain from failure diagnostics",
      attempts = 0L,
      browser_fallback_used = FALSE,
      scraped_at = as.character(Sys.time())
    )))
  }

  links <- unique(links)
  links <- order_links_by_protocol(links, prefer_https = prefer_https)
  links <- links[seq_len(min(length(links), MAX_PAGES_PER_SITE))]

  domain_unreachable <- list()
  domain_reachability_checked <- list()
  out_records <- vector("list", length = 0)

  for (u in links) {
    u_domain <- extract_domain(u)
    if (!is.na(u_domain) && !is.null(domain_unreachable[[u_domain]]) &&
        isTRUE(domain_unreachable[[u_domain]]) && !is_root_url(u)) {
      if (SCRAPER_VERBOSE == 1) {
        message(sprintf("  -> URL: %s [skipped: domain_unreachable]", u))
      }
      out_records[[length(out_records) + 1]] <- tibble(
        ein = ein_i,
        foundation_name = name_i,
        source_url = u,
        final_url = u,
        status_code = NA_integer_,
        title = "",
        text_clean = "",
        scraped_ok = FALSE,
        error_type = "domain_unreachable_skip",
        error_message = "Skipped because root URL for domain failed with request_error",
        attempts = 0L,
        browser_fallback_used = FALSE,
        scraped_at = as.character(Sys.time())
      )
      next
    }

    if (SCRAPER_VERBOSE == 1) {
      message(sprintf("  -> URL: %s", u))
    }

    fetched <- NULL
    for (attempt in seq_len(MAX_RETRIES + 1L)) {
      Sys.sleep(CRAWL_DELAY)
      fetched <- safe_fetch(u)
      fetched$attempts <- attempt
      if (SCRAPER_VERBOSE == 1 && !isTRUE(fetched$ok) && attempt < (MAX_RETRIES + 1L)) {
        message(sprintf("     retry %d/%d (error_type=%s)", attempt, MAX_RETRIES, fetched$error_type))
      }
      if (isTRUE(fetched$ok)) break
    }

    browser_used <- FALSE
    if (BROWSER_FALLBACK_ENABLED == 1 && should_try_browser_fallback(fetched)) {
      if (SCRAPER_VERBOSE == 1) {
        message("     trying browser fallback (Playwright)")
      }
      browser_res <- safe_browser_fetch(u)
      browser_used <- TRUE
      browser_text_chars <- nchar(coalesce(browser_res$text_clean, ""))
      http_text_chars <- nchar(coalesce(fetched$text_clean, ""))

      if (isTRUE(browser_res$ok) && browser_text_chars > http_text_chars) {
        fetched$status_code <- browser_res$status_code
        fetched$final_url <- browser_res$final_url
        fetched$title <- browser_res$title
        fetched$text_clean <- browser_res$text_clean
        fetched$ok <- TRUE
        fetched$error_type <- ""
        fetched$error_message <- ""
      } else if (!isTRUE(fetched$ok)) {
        fetched$error_type <- browser_res$error_type
        fetched$error_message <- browser_res$error_message
      }
    }

    if (!is.na(u_domain) && is_root_url(u) && !isTRUE(fetched$ok) &&
        identical(fetched$error_type, "request_error")) {
      if (is.null(domain_reachability_checked[[u_domain]])) {
        domain_reachability_checked[[u_domain]] <- TRUE
        domain_unreachable[[u_domain]] <- !probe_domain_reachability(u_domain)
      }
    }

    out_records[[length(out_records) + 1]] <- tibble(
      ein = ein_i,
      foundation_name = name_i,
      source_url = u,
      final_url = fetched$final_url,
      status_code = fetched$status_code,
      title = fetched$title,
      text_clean = fetched$text_clean,
      scraped_ok = fetched$ok,
      error_type = fetched$error_type,
      error_message = fetched$error_message,
      attempts = fetched$attempts,
      browser_fallback_used = browser_used,
      scraped_at = as.character(Sys.time())
    )

    if (SCRAPER_VERBOSE == 1 && isTRUE(fetched$ok)) {
      chars_i <- nchar(coalesce(fetched$text_clean, ""))
      message(sprintf("     success: %s (chars=%d)", fetched$final_url, chars_i))
      if (chars_i >= MIN_TEXT_CHARS_STRONG_SUCCESS) {
        message("     early stop: strong success reached; moving to next foundation")
        break
      }
      if (CONTINUE_IF_THIN_SUCCESS == 1) {
        message("     success is thin; trying remaining candidate URLs as fallback")
      } else {
        message("     early stop: first success accepted; moving to next foundation")
        break
      }
    }
  }

  out_records
}

if (USE_DIAGNOSTIC_DOMAIN_FILTERS == 1L) {
  skip_domains <- domain_reco %>% filter(recommendation == "skip_domain") %>% pull(domain) %>% unique()
  prefer_domains <- domain_reco %>% filter(recommendation == "prefer_https") %>% pull(domain) %>% unique()
  message(sprintf(
    "[01b] Domain diagnostics filters enabled: %d skip_domain, %d prefer_https recommendations loaded.",
    length(skip_domains), length(prefer_domains)
  ))
} else {
  skip_domains <- character(0)
  prefer_domains <- character(0)
  message("[01b] Domain diagnostics filters disabled (USE_DIAGNOSTIC_DOMAIN_FILTERS=0).")
}

if (SCRAPER_WORKERS > 1 && .Platform$OS.type == "windows") {
  warning("[01b] SCRAPER_WORKERS > 1 requested on Windows; falling back to sequential mode")
  SCRAPER_WORKERS <- 1L
}

if (!is.na(SCRAPER_WORKERS) && SCRAPER_WORKERS < 1) SCRAPER_WORKERS <- 1L

if (nrow(fdn) == 0) {
  if (nrow(existing_records) > 0) {
    web_texts <- existing_records %>%
      mutate(
        text_clean = str_squish(coalesce(text_clean, "")),
        final_domain = extract_domain(final_url),
        text_chars = nchar(text_clean)
      )
    message("[01b] No remaining foundations to scrape. Using checkpointed results.")
  } else {
    web_texts <- tibble(
      ein = character(),
      foundation_name = character(),
      source_url = character(),
      final_url = character(),
      status_code = integer(),
      title = character(),
      text_clean = character(),
      scraped_ok = logical(),
      error_type = character(),
      error_message = character(),
      attempts = integer(),
      browser_fallback_used = logical(),
      scraped_at = character(),
      final_domain = character(),
      text_chars = integer()
    )
  }
} else {
  idx <- seq_len(nrow(fdn))
  n_workers_effective <- min(SCRAPER_WORKERS, nrow(fdn))
  progress_total <- nrow(fdn)
  progress_done <- 0L
  progress_started_at <- Sys.time()
  progress_last_log_at <- as.POSIXct(NA)

  update_progress <- function(done, force_log = FALSE) {
    utils::setTxtProgressBar(pb, done)
    now <- Sys.time()
    should_log <- force_log || is.na(progress_last_log_at) ||
      as.numeric(difftime(now, progress_last_log_at, units = "secs")) >= 10
    if (!should_log) return(invisible(NULL))

    elapsed <- as.numeric(difftime(now, progress_started_at, units = "secs"))
    eta <- if (done > 0) (elapsed / done) * (progress_total - done) else NA_real_
    pct <- 100 * done / progress_total
    message(sprintf(
      "[01b] Progress %d/%d (%.1f%%) | elapsed %s | ETA %s",
      done, progress_total, pct, format_hms(elapsed), format_hms(eta)
    ))
    progress_last_log_at <<- now
    invisible(NULL)
  }

  pb <- utils::txtProgressBar(min = 0, max = progress_total, style = 3)
  if (SCRAPER_VERBOSE == 1) {
    message(sprintf(
      "[01b] Running scraper with %d worker(s). Browser fallback: %s",
      n_workers_effective,
      ifelse(BROWSER_FALLBACK_ENABLED == 1 && browser_fallback_available, "enabled", "disabled")
    ))
  }
  update_progress(progress_done, force_log = TRUE)

  if (n_workers_effective > 1) {
    nested_records <- vector("list", progress_total)
    foundation_since_checkpoint <- 0L
    checkpoint_buffer <- list()
    cl <- tryCatch(
      parallel::makeCluster(n_workers_effective, type = "PSOCK"),
      error = function(e) e
    )

    if (inherits(cl, "error")) {
      warning(sprintf(
        "[01b] Parallel cluster unavailable (%s). Falling back to sequential mode.",
        conditionMessage(cl)
      ))
      for (i in idx) {
        rec_i <- process_foundation_row(fdn[i, ], i, nrow(fdn), skip_domains, prefer_domains)
        nested_records[[i]] <- rec_i
        checkpoint_buffer[[length(checkpoint_buffer) + 1]] <- bind_rows(rec_i)
        foundation_since_checkpoint <- foundation_since_checkpoint + 1L
        if (foundation_since_checkpoint >= SCRAPER_CHECKPOINT_EVERY) {
          write_checkpoint_rows(bind_rows(checkpoint_buffer))
          checkpoint_buffer <- list()
          foundation_since_checkpoint <- 0L
        }
        progress_done <- progress_done + 1L
        update_progress(progress_done, force_log = FALSE)
      }
      if (length(checkpoint_buffer) > 0) {
        write_checkpoint_rows(bind_rows(checkpoint_buffer))
      }
    } else {
      on.exit(parallel::stopCluster(cl), add = TRUE)
      parallel::clusterEvalQ(cl, {
        library(readr)
        library(dplyr)
        library(stringr)
        library(tidyr)
        library(scales)
        library(httr2)
        library(rvest)
        library(xml2)
        NULL
      })
      parallel::clusterExport(
        cl,
        varlist = c(
          "fdn", "skip_domains", "prefer_domains", "process_foundation_row"
        ),
        envir = environment()
      )

      for (batch_start in seq(1L, progress_total, by = n_workers_effective)) {
        batch_idx <- idx[batch_start:min(progress_total, batch_start + n_workers_effective - 1L)]
        batch_res <- parallel::parLapply(cl, batch_idx, function(i) {
          tryCatch(
            process_foundation_row(fdn[i, ], i, nrow(fdn), skip_domains, prefer_domains),
            error = function(e) {
              list(tibble(
                ein = fdn$ein[i],
                foundation_name = stringr::str_squish(dplyr::coalesce(fdn$foundation_name[i], "")),
                source_url = fdn$seed_url[i],
                final_url = fdn$seed_url[i],
                status_code = NA_integer_,
                title = "",
                text_clean = "",
                scraped_ok = FALSE,
                error_type = "worker_error",
                error_message = conditionMessage(e),
                attempts = 0L,
                browser_fallback_used = FALSE,
                scraped_at = as.character(Sys.time())
              ))
            }
          )
        })

        for (k in seq_along(batch_idx)) {
          i <- batch_idx[k]
          nested_records[[i]] <- batch_res[[k]]
          progress_done <- progress_done + 1L
          update_progress(progress_done, force_log = FALSE)
        }
        write_checkpoint_rows(bind_rows(unlist(batch_res, recursive = FALSE)))
      }
    }
  } else {
    nested_records <- vector("list", progress_total)
    foundation_since_checkpoint <- 0L
    checkpoint_buffer <- list()
    for (i in idx) {
      rec_i <- process_foundation_row(fdn[i, ], i, nrow(fdn), skip_domains, prefer_domains)
      nested_records[[i]] <- rec_i
      checkpoint_buffer[[length(checkpoint_buffer) + 1]] <- bind_rows(rec_i)
      foundation_since_checkpoint <- foundation_since_checkpoint + 1L
      if (foundation_since_checkpoint >= SCRAPER_CHECKPOINT_EVERY) {
        write_checkpoint_rows(bind_rows(checkpoint_buffer))
        checkpoint_buffer <- list()
        foundation_since_checkpoint <- 0L
      }
      progress_done <- progress_done + 1L
      update_progress(progress_done, force_log = FALSE)
    }
    if (length(checkpoint_buffer) > 0) {
      write_checkpoint_rows(bind_rows(checkpoint_buffer))
    }
  }
  close(pb)
  update_progress(progress_done, force_log = TRUE)

  records <- unlist(nested_records, recursive = FALSE)
  new_rows_df <- if (length(records) == 0) tibble() else bind_rows(records)
  new_rows_df <- normalize_web_rows(new_rows_df)
  combined_rows <- if (nrow(existing_records) > 0) bind_rows(existing_records, new_rows_df) else new_rows_df
  if (nrow(combined_rows) == 0) {
    web_texts <- tibble(
      ein = character(),
      foundation_name = character(),
      source_url = character(),
      final_url = character(),
      status_code = integer(),
      title = character(),
      text_clean = character(),
      scraped_ok = logical(),
      error_type = character(),
      error_message = character(),
      attempts = integer(),
      browser_fallback_used = logical(),
      scraped_at = character(),
      final_domain = character(),
      text_chars = integer()
    )
  } else {
    web_texts <- combined_rows %>%
      mutate(
        text_clean = str_squish(coalesce(text_clean, "")),
        final_domain = extract_domain(final_url),
        text_chars = nchar(text_clean)
      )
  }
}

write_csv(web_texts, file_web_texts)
if (file.exists(SCRAPER_CHECKPOINT_FILE)) {
  unlink(SCRAPER_CHECKPOINT_FILE)
}

failures <- web_texts %>%
  filter(!scraped_ok) %>%
  select(ein, source_url, final_url, status_code, error_type, error_message, attempts, scraped_at)
write_csv(failures, file_web_text_failures)

scrape_audit <- web_texts %>%
  group_by(ein, foundation_name) %>%
  summarize(
    urls_attempted = n(),
    urls_succeeded = sum(scraped_ok, na.rm = TRUE),
    any_success = urls_succeeded > 0,
    best_final_url = {
      ok_idx <- which(scraped_ok %in% TRUE)
      if (length(ok_idx) == 0) NA_character_ else final_url[ok_idx[which.max(text_chars[ok_idx])]]
    },
    best_text_chars = {
      ok_chars <- text_chars[scraped_ok %in% TRUE]
      if (length(ok_chars) == 0) NA_integer_ else max(ok_chars, na.rm = TRUE)
    },
    top_error_type = {
      err <- names(sort(table(error_type[!scraped_ok]), decreasing = TRUE))
      if (length(err) == 0) NA_character_ else err[1]
    },
    browser_fallback_uses = sum(browser_fallback_used, na.rm = TRUE),
    .groups = "drop"
  )
write_csv(scrape_audit, file_web_scrape_audit)

summary_tbl <- web_texts %>%
  summarize(
    n_rows = n(),
    n_ein = n_distinct(ein),
    n_ok = sum(scraped_ok, na.rm = TRUE),
    n_failed = sum(!scraped_ok, na.rm = TRUE),
    pct_ok = mean(scraped_ok, na.rm = TRUE),
    avg_text_chars = mean(nchar(text_clean), na.rm = TRUE),
    n_browser_fallback_used = sum(browser_fallback_used, na.rm = TRUE)
  )

write_csv(summary_tbl, file_web_texts_summary)
message("[01b] Done. Wrote: ", file_web_texts)
