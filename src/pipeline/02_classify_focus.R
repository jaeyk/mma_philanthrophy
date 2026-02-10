source(file.path("src", "pipeline", "00_config.R"))
source(file.path("src", "pipeline", "utils.R"))

message("[02] Classifying issue/geographic/demographic focus...")

score_cols <- c(
  "arts", "civic", "community", "econ", "education", "foundations",
  "health", "hobby", "housing", "professional", "regligious",
  "research", "socialfraternal", "unions", "youth"
)

fdn <- read_csv(file_foundation_universe, show_col_types = FALSE)

if (!all(score_cols %in% names(fdn))) {
  stop("Missing issue score columns in foundation universe.")
}

# Issue focus from prediction score vectors.
issue_ranked <- fdn %>%
  select(ein, all_of(score_cols)) %>%
  pivot_longer(cols = all_of(score_cols), names_to = "issue", values_to = "score") %>%
  group_by(ein) %>%
  arrange(desc(score), .by_group = TRUE) %>%
  mutate(rank = row_number()) %>%
  ungroup()

issue_top <- issue_ranked %>%
  filter(rank <= 3) %>%
  select(ein, rank, issue, score) %>%
  mutate(rank_label = paste0("issue_rank", rank)) %>%
  pivot_wider(names_from = rank_label, values_from = c(issue, score))

issue_entropy <- fdn %>%
  rowwise() %>%
  transmute(ein, issue_entropy = entropy(c_across(all_of(score_cols))))

# Geographic and demographic focus via weak supervision from name/domain/url text.
focus_text <- fdn %>%
  transmute(
    ein,
    focus_text = str_to_lower(str_c(
      coalesce(name, ""), " ",
      coalesce(taxpayer_name, ""), " ",
      coalesce(domain, ""), " ",
      coalesce(candidate_url, "")
    ))
  )

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
    issue_focus_primary = issue_issue_rank1,
    issue_focus_secondary = issue_issue_rank2,
    issue_focus_tertiary = issue_issue_rank3
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

