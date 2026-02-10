source(file.path("src", "pipeline", "00_config.R"))
source(file.path("src", "pipeline", "utils.R"))

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

fdn <- read_csv(file_foundation_universe, show_col_types = FALSE) %>%
  filter(url_quality_keep, !is.na(candidate_url), candidate_url != "") %>%
  mutate(
    seed_url = candidate_url,
    seed_domain = extract_domain(seed_url),
    foundation_name = coalesce(name, taxpayer_name, "")
  ) %>%
  distinct(ein, .keep_all = TRUE)

MAX_ORGS <- as.integer(Sys.getenv("MAX_ORGS", unset = "5000"))
MAX_PAGES_PER_SITE <- as.integer(Sys.getenv("MAX_PAGES_PER_SITE", unset = "4"))
REQUEST_TIMEOUT <- as.integer(Sys.getenv("REQUEST_TIMEOUT_SECONDS", unset = "20"))
CRAWL_DELAY <- as.numeric(Sys.getenv("CRAWL_DELAY_SECONDS", unset = "0.4"))
MAX_RETRIES <- as.integer(Sys.getenv("MAX_RETRIES", unset = "2"))
SCRAPER_VERBOSE <- as.integer(Sys.getenv("SCRAPER_VERBOSE", unset = "1"))

if (nrow(fdn) > MAX_ORGS) {
  fdn <- fdn %>% slice_head(n = MAX_ORGS)
}

extract_text_from_html <- function(html_raw) {
  doc <- xml2::read_html(html_raw)
  title <- rvest::html_text2(rvest::html_element(doc, "title"))
  body_txt <- rvest::html_text2(rvest::html_element(doc, "body"))
  body_txt <- str_squish(body_txt)
  body_txt <- str_replace_all(body_txt, "\\s+", " ")
  list(title = coalesce(title, ""), text = body_txt)
}

safe_fetch <- function(url) {
  out <- list(
    status_code = NA_integer_, final_url = url, title = "", text_clean = "",
    ok = FALSE, error_type = "", error_message = "", attempts = 0L
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
  split_links <- function(x) {
    if (is.null(x) || is.na(x) || x == "") return(character(0))
    parts <- unlist(str_split(x, ",", simplify = FALSE))
    parts <- str_squish(parts)
    parts[parts != ""]
  }
  pool <- c(
    row$candidate_url,
    row$preferred_link,
    row$first_deep_link,
    row$irs_url,
    row$first_link,
    split_links(row$all_deep_links)
  )
  pool <- pool[!is.na(pool) & pool != ""]
  # Deduplicate by normalized URL but keep original first occurrence.
  if (length(pool) == 0) return(character(0))
  norm <- normalize_url(pool)
  pool[!duplicated(norm)]
}

records <- vector("list", length = 0)
pb <- utils::txtProgressBar(min = 0, max = nrow(fdn), style = 3)

for (i in seq_len(nrow(fdn))) {
  ein_i <- fdn$ein[i]
  seed_i <- fdn$seed_url[i]
  domain_i <- fdn$seed_domain[i]
  name_i <- str_squish(coalesce(fdn$foundation_name[i], ""))
  if (is.na(seed_i) || seed_i == "") next

  if (SCRAPER_VERBOSE == 1) {
    message(sprintf("[01b] [%d/%d] EIN %s | %s", i, nrow(fdn), ein_i, name_i))
  }

  row_i <- fdn[i, ]
  seed_pool <- build_seed_pool(row_i)
  if (length(seed_pool) == 0) {
    utils::setTxtProgressBar(pb, i)
    next
  }

  seed_pool <- seed_pool[seq_len(min(length(seed_pool), 3))]
  links <- character(0)
  doms <- character(0)
  for (s in seed_pool) {
    d <- extract_domain(s)
    if (!is.na(d) && d != "") doms <- c(doms, d)
    links <- c(links, collect_candidate_links(s, d))
  }
  doms <- unique(str_to_lower(doms))
  reco_i <- domain_reco %>% filter(domain %in% doms)
  should_skip_domain <- any(reco_i$recommendation == "skip_domain")
  prefer_https <- any(reco_i$recommendation == "prefer_https")

  if (should_skip_domain) {
    if (SCRAPER_VERBOSE == 1) {
      message("  -> skipped entire domain by diagnostics recommendation (skip_domain)")
    }
    records[[length(records) + 1]] <- tibble(
      ein = ein_i,
      source_url = seed_i,
      final_url = seed_i,
      status_code = NA_integer_,
      title = "",
      text_clean = "",
      scraped_ok = FALSE,
      error_type = "diagnostic_skip_domain",
      error_message = "Domain marked skip_domain from failure diagnostics",
      attempts = 0L,
      scraped_at = as.character(Sys.time())
    )
    utils::setTxtProgressBar(pb, i)
    next
  }

  links <- unique(links)
  links <- order_links_by_protocol(links, prefer_https = prefer_https)
  links <- links[seq_len(min(length(links), MAX_PAGES_PER_SITE))]
  domain_unreachable <- list()
  domain_reachability_checked <- list()

  for (u in links) {
    u_domain <- extract_domain(u)
    if (!is.na(u_domain) && !is.null(domain_unreachable[[u_domain]]) &&
        isTRUE(domain_unreachable[[u_domain]]) && !is_root_url(u)) {
      if (SCRAPER_VERBOSE == 1) {
        message(sprintf("  -> URL: %s [skipped: domain_unreachable]", u))
      }
      records[[length(records) + 1]] <- tibble(
        ein = ein_i,
        source_url = u,
        final_url = u,
        status_code = NA_integer_,
        title = "",
        text_clean = "",
        scraped_ok = FALSE,
        error_type = "domain_unreachable_skip",
        error_message = "Skipped because root URL for domain failed with request_error",
        attempts = 0L,
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

    if (!is.na(u_domain) && is_root_url(u) && !isTRUE(fetched$ok) &&
        identical(fetched$error_type, "request_error")) {
      if (is.null(domain_reachability_checked[[u_domain]])) {
        domain_reachability_checked[[u_domain]] <- TRUE
        domain_unreachable[[u_domain]] <- !probe_domain_reachability(u_domain)
      }
    }

    records[[length(records) + 1]] <- tibble(
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
      scraped_at = as.character(Sys.time())
    )

    if (SCRAPER_VERBOSE == 1 && isTRUE(fetched$ok)) {
      message(sprintf("     success: %s (chars=%d)", fetched$final_url, nchar(coalesce(fetched$text_clean, ""))))
    }
  }

  utils::setTxtProgressBar(pb, i)
}
close(pb)

web_texts <- bind_rows(records) %>%
  mutate(
    text_clean = str_squish(coalesce(text_clean, "")),
    final_domain = extract_domain(final_url),
    text_chars = nchar(text_clean)
  )

write_csv(web_texts, file_web_texts)

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
    best_final_url = if_else(any_success, final_url[which.max(text_chars * as.integer(scraped_ok))], NA_character_),
    best_text_chars = if_else(any_success, max(text_chars[scraped_ok], na.rm = TRUE), NA_integer_),
    top_error_type = {
      err <- names(sort(table(error_type[!scraped_ok]), decreasing = TRUE))
      if (length(err) == 0) NA_character_ else err[1]
    },
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
    avg_text_chars = mean(nchar(text_clean), na.rm = TRUE)
  )

write_csv(summary_tbl, file_web_texts_summary)
message("[01b] Done. Wrote: ", file_web_texts)
