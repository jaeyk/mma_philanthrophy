source(file.path("src", "pipeline", "00_config.R"))
source(file.path("src", "pipeline", "utils.R"))

message("[02b] Scoring democracy/state-capacity support constructs...")

if (!file.exists(file_construct_taxonomy)) {
  stop("Missing taxonomy file: ", file_construct_taxonomy)
}

fdn <- read_csv(file_foundation_universe, show_col_types = FALSE)
taxonomy <- read_csv(file_construct_taxonomy, show_col_types = FALSE) %>%
  mutate(
    construct = str_to_lower(construct),
    subdimension = str_to_lower(subdimension),
    polarity = str_to_lower(polarity),
    weight = as.numeric(weight)
  )

# Build available weak-text field from current data assets.
text_df <- fdn %>%
  mutate(
    issue1_text = if ("issue_issue_rank1" %in% names(.)) coalesce(as.character(issue_issue_rank1), "") else "",
    issue2_text = if ("issue_issue_rank2" %in% names(.)) coalesce(as.character(issue_issue_rank2), "") else "",
    issue3_text = if ("issue_issue_rank3" %in% names(.)) coalesce(as.character(issue_issue_rank3), "") else ""
  ) %>%
  transmute(
    ein,
    signal_text = str_to_lower(str_c(
      coalesce(name, ""), " ",
      coalesce(taxpayer_name, ""), " ",
      coalesce(candidate_url, ""), " ",
      coalesce(domain, ""), " ",
      coalesce(ntee_cd, ""), " ",
      issue1_text, " ",
      issue2_text, " ",
      issue3_text
    ))
  ) %>%
  distinct(ein, .keep_all = TRUE)

matched <- text_df %>%
  crossing(taxonomy) %>%
  mutate(is_match = str_detect(signal_text, regex(pattern, ignore_case = TRUE))) %>%
  filter(is_match) %>%
  mutate(
    signed_weight = if_else(polarity == "negative", -abs(weight), abs(weight))
  )

construct_scores <- matched %>%
  group_by(ein, construct) %>%
  summarize(
    score = sum(signed_weight, na.rm = TRUE),
    n_matches = n(),
    matched_patterns = str_c(unique(pattern), collapse = " | "),
    matched_subdimensions = str_c(unique(subdimension), collapse = ";"),
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from = construct,
    values_from = c(score, n_matches, matched_patterns, matched_subdimensions),
    values_fill = list(score = 0, n_matches = 0, matched_patterns = "", matched_subdimensions = "")
  )

scores_out <- fdn %>%
  select(ein, ruca_category, city_size_tier, assets, income, revenue) %>%
  distinct(ein, .keep_all = TRUE) %>%
  left_join(construct_scores, by = "ein") %>%
  mutate(
    score_democracy = coalesce(score_democracy, 0),
    score_state_capacity = coalesce(score_state_capacity, 0),
    n_matches_democracy = coalesce(n_matches_democracy, 0),
    n_matches_state_capacity = coalesce(n_matches_state_capacity, 0),
    # Conservative default thresholds for publication-facing descriptive work.
    democracy_support_flag = score_democracy >= 2,
    state_capacity_support_flag = score_state_capacity >= 2,
    democracy_support_conf = case_when(
      score_democracy >= 5 ~ "high",
      score_democracy >= 2 ~ "medium",
      score_democracy > 0 ~ "low",
      TRUE ~ "none"
    ),
    state_capacity_support_conf = case_when(
      score_state_capacity >= 5 ~ "high",
      score_state_capacity >= 2 ~ "medium",
      score_state_capacity > 0 ~ "low",
      TRUE ~ "none"
    ),
    support_intersection = case_when(
      democracy_support_flag & state_capacity_support_flag ~ "both",
      democracy_support_flag & !state_capacity_support_flag ~ "democracy_only",
      !democracy_support_flag & state_capacity_support_flag ~ "state_capacity_only",
      TRUE ~ "neither_or_unclear"
    )
  )

write_csv(scores_out, file_construct_scores)

summary_construct <- scores_out %>%
  summarize(
    n = n(),
    n_democracy_support = sum(democracy_support_flag, na.rm = TRUE),
    n_state_capacity_support = sum(state_capacity_support_flag, na.rm = TRUE),
    n_both = sum(support_intersection == "both", na.rm = TRUE),
    pct_democracy_support = mean(democracy_support_flag, na.rm = TRUE),
    pct_state_capacity_support = mean(state_capacity_support_flag, na.rm = TRUE),
    pct_both = mean(support_intersection == "both", na.rm = TRUE)
  )

write_csv(summary_construct, file.path(path_final, "construct_support_summary.csv"))
write_csv(scores_out %>% count(support_intersection, sort = TRUE),
          file.path(path_final, "construct_support_intersection_distribution.csv"))

message("[02b] Done. Wrote: ", file_construct_scores)
