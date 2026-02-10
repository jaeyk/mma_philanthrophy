source(file.path("src", "pipeline", "00_config.R"))
source(file.path("src", "pipeline", "utils.R"))

message("[02c] Scoring tech/AI/innovation/entrepreneurship constructs...")

if (!file.exists(file_taie_taxonomy)) {
  stop("Missing taxonomy file: ", file_taie_taxonomy)
}

fdn <- read_csv(file_foundation_universe, show_col_types = FALSE)
taxonomy <- read_csv(file_taie_taxonomy, show_col_types = FALSE) %>%
  mutate(
    construct = str_to_lower(construct),
    subdimension = str_to_lower(subdimension),
    polarity = str_to_lower(polarity),
    weight = as.numeric(weight)
  )

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
  mutate(signed_weight = if_else(polarity == "negative", -abs(weight), abs(weight)))

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
    score_tech = coalesce(score_tech, 0),
    score_ai = coalesce(score_ai, 0),
    score_innovation = coalesce(score_innovation, 0),
    score_entrepreneurship = coalesce(score_entrepreneurship, 0),
    tech_interest_flag = score_tech >= 2,
    ai_interest_flag = score_ai >= 2,
    innovation_interest_flag = score_innovation >= 2,
    entrepreneurship_interest_flag = score_entrepreneurship >= 2,
    taie_count = as.integer(tech_interest_flag) + as.integer(ai_interest_flag) +
      as.integer(innovation_interest_flag) + as.integer(entrepreneurship_interest_flag),
    taie_profile = case_when(
      taie_count == 0 ~ "none",
      taie_count == 1 ~ "single_focus",
      taie_count == 2 ~ "dual_focus",
      taie_count == 3 ~ "triple_focus",
      taie_count >= 4 ~ "all_four"
    )
  )

write_csv(scores_out, file_taie_scores)

summary_taie <- scores_out %>%
  summarize(
    n = n(),
    pct_tech = mean(tech_interest_flag, na.rm = TRUE),
    pct_ai = mean(ai_interest_flag, na.rm = TRUE),
    pct_innovation = mean(innovation_interest_flag, na.rm = TRUE),
    pct_entrepreneurship = mean(entrepreneurship_interest_flag, na.rm = TRUE),
    pct_any_taie = mean(taie_count >= 1, na.rm = TRUE)
  )

write_csv(summary_taie, file.path(path_final, "taie_support_summary.csv"))
write_csv(scores_out %>% count(taie_profile, sort = TRUE),
          file.path(path_final, "taie_profile_distribution.csv"))

long_flags <- bind_rows(
  scores_out %>% transmute(ein, construct = "tech", flag = tech_interest_flag),
  scores_out %>% transmute(ein, construct = "ai", flag = ai_interest_flag),
  scores_out %>% transmute(ein, construct = "innovation", flag = innovation_interest_flag),
  scores_out %>% transmute(ein, construct = "entrepreneurship", flag = entrepreneurship_interest_flag)
) %>%
  group_by(construct) %>%
  summarize(n = sum(flag, na.rm = TRUE), pct = mean(flag, na.rm = TRUE), .groups = "drop")

write_csv(long_flags, file.path(path_final, "taie_construct_distribution.csv"))

message("[02c] Done. Wrote: ", file_taie_scores)

