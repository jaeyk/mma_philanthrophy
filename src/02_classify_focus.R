source(file.path("src", "00_config.R"))
source(file.path("src", "utils.R"))

message("[02] Classifying issue/geographic/demographic focus...")

fdn <- read_csv(file_foundation_universe, show_col_types = FALSE) %>%
  distinct(ein, .keep_all = TRUE)
if (!file.exists(file_issue_taxonomy)) {
  stop("Missing issue taxonomy file: ", file_issue_taxonomy)
}

issue_taxonomy <- read_csv(file_issue_taxonomy, show_col_types = FALSE) %>%
  mutate(
    issue = str_to_lower(issue),
    pattern = as.character(pattern),
    weight = as.numeric(weight),
    polarity = str_to_lower(polarity)
  )

# Geographic and demographic focus via weak supervision from reusable signal text.
focus_text <- build_signal_text(fdn, file_web_texts) %>%
  rename(focus_text = signal_text)

# Issue focus from scraped/aggregated text using weighted pattern matching.
issue_matches <- focus_text %>%
  crossing(issue_taxonomy) %>%
  mutate(
    match = str_detect(focus_text, regex(pattern, ignore_case = TRUE)),
    signed_weight = if_else(polarity == "negative", -abs(weight), abs(weight))
  ) %>%
  filter(match)

issue_scores <- issue_matches %>%
  group_by(ein, issue) %>%
  summarize(
    issue_score = sum(signed_weight, na.rm = TRUE),
    issue_n_matches = n(),
    issue_patterns = str_c(unique(pattern), collapse = " | "),
    .groups = "drop"
  ) %>%
  mutate(issue_score = pmax(issue_score, 0))

issue_ranked <- issue_scores %>%
  group_by(ein) %>%
  arrange(desc(issue_score), desc(issue_n_matches), .by_group = TRUE) %>%
  mutate(rank = row_number()) %>%
  ungroup()

issue_top <- issue_ranked %>%
  filter(rank <= 3) %>%
  select(ein, rank, issue, issue_score) %>%
  mutate(rank_label = paste0("issue_rank", rank)) %>%
  pivot_wider(names_from = rank_label, values_from = c(issue, issue_score)) %>%
  rename(
    issue_label_rank1 = issue_issue_rank1,
    issue_label_rank2 = issue_issue_rank2,
    issue_label_rank3 = issue_issue_rank3,
    issue_score_rank1 = issue_score_issue_rank1,
    issue_score_rank2 = issue_score_issue_rank2,
    issue_score_rank3 = issue_score_issue_rank3
  )

issue_entropy <- issue_scores %>%
  group_by(ein) %>%
  summarize(issue_entropy = entropy(issue_score), .groups = "drop")

geo_focus <- focus_text %>%
  transmute(
    ein,
    geo_focus = case_when(
      str_detect(focus_text, "international|global|worldwide|africa|asia|latin america|overseas") ~ "international",
      str_detect(focus_text, "national|nationwide|across the us|united states") ~ "national",
      str_detect(focus_text, "statewide|county|city of|local|community") ~ "local_or_regional",
      TRUE ~ "unknown"
    )
  )

demographic_patterns <- tribble(
  ~demographic_focus, ~pattern,
  "youth_children", "youth|children|kids|teen|adolescent|early childhood",
  "women_girls", "women|woman|girls|girl",
  "older_adults", "senior|elder|aging|older adult",
  "black_communities", "black|african american",
  "latino_hispanic", "latino|latina|hispanic",
  "asian_communities", "asian|aapi|pacific islander",
  "native_indigenous", "native american|indigenous|tribal",
  "immigrants_refugees", "immigrant|refugee|asylum",
  "disability", "disability|disabled|special needs|blind|deaf",
  "veterans", "veteran|military family",
  "low_income_poverty", "poverty|low-income|homeless|economic hardship"
)

demog_long <- focus_text %>%
  crossing(demographic_patterns) %>%
  mutate(match = str_detect(focus_text, pattern)) %>%
  filter(match) %>%
  group_by(ein) %>%
  summarize(
    demographic_focus_labels = str_c(sort(unique(demographic_focus)), collapse = ";"),
    demographic_focus_n = n_distinct(demographic_focus),
    .groups = "drop"
  )

classified <- fdn %>%
  left_join(issue_top, by = "ein") %>%
  left_join(issue_entropy, by = "ein") %>%
  left_join(geo_focus, by = "ein") %>%
  left_join(demog_long, by = "ein") %>%
  mutate(
    demographic_focus_labels = coalesce(demographic_focus_labels, "unknown"),
    demographic_focus_n = coalesce(demographic_focus_n, 0L),
    geo_focus = coalesce(geo_focus, "unknown"),
    issue_focus_primary = coalesce(issue_label_rank1, "unknown"),
    issue_focus_secondary = coalesce(issue_label_rank2, "unknown"),
    issue_focus_tertiary = coalesce(issue_label_rank3, "unknown")
  )

write_csv(classified, file_focus_classified)

focus_summary <- list(
  issue_primary = classified %>% count(issue_focus_primary, sort = TRUE),
  geo_focus = classified %>% count(geo_focus, sort = TRUE),
  demographic_label = classified %>% count(demographic_focus_labels, sort = TRUE) %>% slice_head(n = 25)
)

write_csv(focus_summary$issue_primary, file.path(path_final, "focus_issue_primary_distribution.csv"))
write_csv(focus_summary$geo_focus, file.path(path_final, "focus_geo_distribution.csv"))
write_csv(focus_summary$demographic_label, file.path(path_final, "focus_demographic_distribution_top25.csv"))

message("[02] Done. Wrote: ", file_focus_classified)
