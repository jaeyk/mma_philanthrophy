source(file.path("src", "00_config.R"))
source(file.path("src", "utils.R"))

message("[01] Building foundation universe...")

score_cols <- c(
  "arts", "civic", "community", "econ", "education", "foundations",
  "health", "hobby", "housing", "professional", "regligious",
  "research", "socialfraternal", "unions", "youth"
)

preds <- read_csv(
  file_predictions,
  col_select = c(ein, predicted, irs_class, lat, lng, fnd_yr, all_of(score_cols)),
  show_col_types = FALSE
)

mbf <- read_csv(
  file_mbf,
  col_select = c(ein, name, city, state, zip, ntee_cd, asset_amt, income_amt, revenue_amt),
  show_col_types = FALSE
)

fips <- read_csv(
  file_fips,
  col_select = c(ein, fips),
  show_col_types = FALSE
)

urls <- read_csv(
  file_urls,
  col_select = c(
    ein, taxpayer_name, first_link, first_deep_link, all_deep_links, found_deep_link,
    irs_url, preferred_link, first_exclude, deep_exclude, irs_exclude, url_check
  ),
  show_col_types = FALSE
)

url_web <- read_csv(
  file_url_web,
  col_select = c(url, donations, events, membership, digital_membership, chapters, volunteer,
                 services, take_action, advocacy, visit, resources, board, press),
  show_col_types = FALSE
)

ruca <- fetch_ruca(file_ruca_cache, ruca_url)

blocked_domain_patterns <- c(
  "guidestar.org$", "nonprofitfacts.com$", "charitynavigator.org$", "charities.pinkaloo.com$",
  "facebook.com$", "instagram.com$", "linkedin.com$", "twitter.com$", "x.com$", "youtube.com$",
  "yelp.com$", "google\\.", "bing.com$", "yahoo.com$", "mapquest.com$", "bizapedia.com$",
  "dandb.com$", "zoominfo.com$", "wikipedia.org$"
)
blocked_regex <- paste0("(", paste(blocked_domain_patterns, collapse = "|"), ")")

url_web_norm <- url_web %>%
  mutate(url_norm = normalize_url(url)) %>%
  filter(!is.na(url_norm), url_norm != "") %>%
  distinct(url_norm, .keep_all = TRUE)

foundation <- preds %>%
  filter(predicted == "foundation") %>%
  left_join(mbf, by = "ein") %>%
  left_join(fips, by = "ein") %>%
  left_join(urls, by = "ein") %>%
  mutate(
    city = str_to_upper(str_trim(city)),
    state = str_to_upper(str_trim(state)),
    zip5 = str_sub(str_pad(as.character(zip), 5, side = "left", pad = "0"), 1, 5),
    lat = safe_numeric(lat),
    lng = safe_numeric(lng),
    assets = safe_numeric(asset_amt),
    income = safe_numeric(income_amt),
    revenue = safe_numeric(revenue_amt),
    # Better seed priority:
    # 1) validated preferred_link (url_check == t), 2) deep link, 3) IRS URL, 4) first_link.
    candidate_url = case_when(
      str_to_lower(coalesce(as.character(url_check), "")) == "t" &
        !is.na(preferred_link) & preferred_link != "" ~ preferred_link,
      found_deep_link == 1 & deep_exclude == 0 &
        !is.na(first_deep_link) & first_deep_link != "" ~ first_deep_link,
      !is.na(irs_url) & irs_url != "" & irs_exclude == 0 ~ irs_url,
      !is.na(first_link) & first_link != "" & first_exclude == 0 ~ first_link,
      !is.na(preferred_link) & preferred_link != "" ~ preferred_link,
      !is.na(first_deep_link) & first_deep_link != "" ~ first_deep_link,
      !is.na(irs_url) & irs_url != "" ~ irs_url,
      !is.na(first_link) & first_link != "" ~ first_link,
      TRUE ~ NA_character_
    ),
    url_source = case_when(
      !is.na(preferred_link) & preferred_link != "" ~ "preferred_link",
      !is.na(irs_url) & irs_url != "" ~ "irs_url",
      !is.na(first_link) & first_link != "" ~ "first_link",
      TRUE ~ "none"
    ),
    url_norm = normalize_url(candidate_url),
    domain = extract_domain(candidate_url),
    has_candidate_url = !is.na(candidate_url) & candidate_url != "",
    source_excluded = case_when(
      url_source == "irs_url" ~ irs_exclude == 1,
      url_source == "first_link" ~ first_exclude == 1,
      TRUE ~ FALSE
    ),
    failed_check = str_to_lower(coalesce(as.character(url_check), "")) == "f",
    malformed_domain = is.na(domain) | !str_detect(domain, "\\."),
    blocked_domain = str_detect(coalesce(domain, ""), blocked_regex),
    url_quality_keep = has_candidate_url & !source_excluded & !failed_check & !malformed_domain & !blocked_domain
  ) %>%
  left_join(url_web_norm %>% mutate(has_web_features = TRUE), by = "url_norm") %>%
  mutate(has_web_features = coalesce(has_web_features, FALSE)) %>%
  left_join(ruca, by = "zip5") %>%
  mutate(
    # Coarse city-size proxy derived from RUCA classes.
    city_size_tier = case_when(
      ruca_category == "Metropolitan" ~ "large_metro",
      ruca_category == "Micropolitan" ~ "mid_small_metro",
      ruca_category %in% c("Small town", "Rural") ~ "small_town_or_rural",
      TRUE ~ NA_character_
    )
  )

write_csv(foundation, file_foundation_universe)

summary_tbl <- foundation %>%
  summarize(
    n_foundations = n(),
    pct_with_coords = mean(!is.na(lat) & !is.na(lng)),
    pct_with_assets = mean(!is.na(assets) & assets > 0),
    pct_with_url_candidate = mean(has_candidate_url),
    pct_url_quality_keep = mean(url_quality_keep),
    pct_url_keep_and_web_features = mean(url_quality_keep & has_web_features),
    pct_with_ruca = mean(!is.na(ruca_category))
  )

write_csv(summary_tbl, file.path(path_final, "foundation_universe_summary.csv"))
message("[01] Done. Wrote: ", file_foundation_universe)
