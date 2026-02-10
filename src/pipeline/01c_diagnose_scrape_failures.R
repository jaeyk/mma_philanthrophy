source(file.path("src", "pipeline", "00_config.R"))
source(file.path("src", "pipeline", "utils.R"))

message("[01c] Diagnosing scrape failures on a random sample...")

if (!requireNamespace("httr2", quietly = TRUE)) {
  stop("Please install required package: httr2")
}

if (!file.exists(file_web_text_failures)) {
  stop("Missing failure file: ", file_web_text_failures)
}

SAMPLE_N <- as.integer(Sys.getenv("FAILURE_SAMPLE_N", unset = "200"))
PROBE_TIMEOUT <- as.integer(Sys.getenv("FAILURE_PROBE_TIMEOUT_SECONDS", unset = "8"))
SEED <- as.integer(Sys.getenv("FAILURE_SAMPLE_SEED", unset = "42"))
set.seed(SEED)

failures <- read_csv(file_web_text_failures, show_col_types = FALSE) %>%
  filter(error_type %in% c("request_error", "domain_unreachable_skip")) %>%
  mutate(domain = extract_domain(source_url)) %>%
  filter(!is.na(domain), domain != "")

domains <- failures %>%
  distinct(domain) %>%
  mutate(sample_rank = row_number())

if (nrow(domains) == 0) {
  write_csv(tibble(), file_web_failure_diagnostic_sample)
  message("[01c] No domains to diagnose.")
  quit(save = "no")
}

sample_domains <- domains %>%
  slice_sample(n = min(SAMPLE_N, n()))

safe_probe <- function(url) {
  out <- list(status_code = NA_integer_, error_type = "", error_message = "")
  resp <- tryCatch({
    httr2::request(url) %>%
      httr2::req_user_agent("foundation-focus-pipeline/0.1-diagnostic") %>%
      httr2::req_timeout(PROBE_TIMEOUT) %>%
      httr2::req_perform()
  }, error = function(e) e)

  if (inherits(resp, "error")) {
    out$error_type <- "request_error"
    out$error_message <- conditionMessage(resp)
    return(out)
  }

  out$status_code <- httr2::resp_status(resp)
  ctype <- httr2::resp_header(resp, "content-type")
  if (is.null(ctype)) ctype <- ""
  if (is.na(ctype)) ctype <- ""
  if (!(grepl("text/html", tolower(ctype), fixed = TRUE) || ctype == "")) {
    out$error_type <- "non_html"
    out$error_message <- ctype
  }
  out
}

probe_domain <- function(domain) {
  urls <- c(
    str_c("https://", domain, "/"),
    str_c("http://", domain, "/"),
    str_c("https://www.", domain, "/"),
    str_c("http://www.", domain, "/")
  )
  urls <- unique(urls)
  res <- lapply(urls, function(u) c(list(url = u), safe_probe(u)))
  tibble(
    domain = domain,
    url = vapply(res, function(x) x$url, character(1)),
    status_code = vapply(res, function(x) ifelse(is.null(x$status_code), NA_integer_, x$status_code), integer(1)),
    error_type = vapply(res, function(x) x$error_type %||% "", character(1)),
    error_message = vapply(res, function(x) x$error_message %||% "", character(1))
  )
}

`%||%` <- function(a, b) if (!is.null(a) && !is.na(a)) a else b

diagnosed <- bind_rows(lapply(sample_domains$domain, probe_domain)) %>%
  group_by(domain) %>%
  mutate(
    any_success = any(!is.na(status_code) & status_code >= 200 & status_code < 500),
    likely_https_only = any(str_starts(url, "https://") & !is.na(status_code) & status_code >= 200 & status_code < 500) &
      !any(str_starts(url, "http://") & !is.na(status_code) & status_code >= 200 & status_code < 500),
    likely_dns_or_dead = all(error_type == "request_error" | is.na(status_code))
  ) %>%
  ungroup()

write_csv(diagnosed, file_web_failure_diagnostic_sample)

summary_tbl <- diagnosed %>%
  distinct(domain, any_success, likely_https_only, likely_dns_or_dead) %>%
  summarize(
    n_domains = n(),
    pct_any_success = mean(any_success),
    pct_likely_https_only = mean(likely_https_only),
    pct_likely_dns_or_dead = mean(likely_dns_or_dead)
  )

write_csv(summary_tbl, file.path(path_final, "foundation_web_failure_diagnostic_summary.csv"))

# Domain-level recommendations for future scraper runs.
domain_reco <- diagnosed %>%
  group_by(domain) %>%
  summarize(
    any_success = any(any_success),
    likely_https_only = any(likely_https_only),
    likely_dns_or_dead = any(likely_dns_or_dead),
    recommendation = case_when(
      likely_dns_or_dead ~ "skip_domain",
      likely_https_only ~ "prefer_https",
      any_success ~ "normal",
      TRUE ~ "normal"
    ),
    .groups = "drop"
  )

write_csv(domain_reco, file_web_failure_domain_recommendations)
message("[01c] Done. Wrote: ", file_web_failure_diagnostic_sample)
