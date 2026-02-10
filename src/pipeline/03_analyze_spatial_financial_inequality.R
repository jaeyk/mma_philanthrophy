source(file.path("src", "pipeline", "00_config.R"))
source(file.path("src", "pipeline", "utils.R"))

message("[03] Producing spatial-financial inequality outputs...")

fdn_base <- read_csv(file_focus_classified, show_col_types = FALSE) %>%
  mutate(
    assets = safe_numeric(assets),
    income = safe_numeric(income),
    revenue = safe_numeric(revenue)
  )

fdn <- if (file.exists(file_construct_scores)) {
  construct_scores <- read_csv(file_construct_scores, show_col_types = FALSE) %>%
    select(ein, democracy_support_flag, state_capacity_support_flag, support_intersection,
           score_democracy, score_state_capacity, democracy_support_conf, state_capacity_support_conf)
  fdn_base %>% left_join(construct_scores, by = "ein")
} else {
  fdn_base
}

if (file.exists(file_taie_scores)) {
  taie_scores <- read_csv(file_taie_scores, show_col_types = FALSE) %>%
    select(
      ein, tech_interest_flag, ai_interest_flag, innovation_interest_flag,
      entrepreneurship_interest_flag, taie_count, taie_profile,
      score_tech, score_ai, score_innovation, score_entrepreneurship
    )
  fdn <- fdn %>% left_join(taie_scores, by = "ein")
}

asset_cut <- fdn %>%
  filter(!is.na(assets), assets > 0) %>%
  summarize(v = as.numeric(quantile(assets, 0.99))) %>%
  pull(v)

fdn <- fdn %>%
  mutate(big_player_assets = !is.na(assets) & assets >= asset_cut)

overall <- fdn %>%
  summarize(
    n = n(),
    asset_top1pct_cutoff = asset_cut,
    gini_assets = gini(assets),
    gini_income = gini(income),
    gini_revenue = gini(revenue),
    top1pct_asset_share = top_share(assets, 0.01),
    top1pct_revenue_share = top_share(revenue, 0.01)
  )
write_csv(overall, file.path(path_final, "inequality_overall.csv"))

by_ruca <- fdn %>%
  filter(!is.na(ruca_category)) %>%
  group_by(ruca_category) %>%
  summarize(
    n = n(),
    n_big_players = sum(big_player_assets, na.rm = TRUE),
    big_player_rate = n_big_players / n,
    median_assets = median(assets[assets > 0], na.rm = TRUE),
    gini_assets = gini(assets),
    top1pct_asset_share = top_share(assets, 0.01),
    .groups = "drop"
  ) %>%
  arrange(desc(big_player_rate))
write_csv(by_ruca, file.path(path_final, "inequality_by_ruca.csv"))

by_city_tier <- fdn %>%
  filter(!is.na(city_size_tier)) %>%
  group_by(city_size_tier) %>%
  summarize(
    n = n(),
    n_big_players = sum(big_player_assets, na.rm = TRUE),
    big_player_rate = n_big_players / n,
    median_assets = median(assets[assets > 0], na.rm = TRUE),
    gini_assets = gini(assets),
    top1pct_asset_share = top_share(assets, 0.01),
    .groups = "drop"
  ) %>%
  arrange(desc(big_player_rate))
write_csv(by_city_tier, file.path(path_final, "inequality_by_city_tier.csv"))

if ("issue_focus_primary" %in% names(fdn)) {
  by_issue <- fdn %>%
    filter(!is.na(issue_focus_primary)) %>%
    group_by(issue_focus_primary) %>%
    summarize(
      n = n(),
      n_big_players = sum(big_player_assets, na.rm = TRUE),
      big_player_rate = n_big_players / n,
      median_assets = median(assets[assets > 0], na.rm = TRUE),
      gini_assets = gini(assets),
      .groups = "drop"
    ) %>%
    filter(n >= 250) %>%
    arrange(desc(big_player_rate))
  write_csv(by_issue, file.path(path_final, "inequality_by_primary_issue.csv"))
}

by_geo_focus <- fdn %>%
  group_by(geo_focus) %>%
  summarize(
    n = n(),
    n_big_players = sum(big_player_assets, na.rm = TRUE),
    big_player_rate = n_big_players / n,
    median_assets = median(assets[assets > 0], na.rm = TRUE),
    gini_assets = gini(assets),
    .groups = "drop"
  ) %>%
  arrange(desc(big_player_rate))
write_csv(by_geo_focus, file.path(path_final, "inequality_by_geo_focus.csv"))

by_demo <- fdn %>%
  filter(demographic_focus_labels != "unknown") %>%
  separate_rows(demographic_focus_labels, sep = ";") %>%
  group_by(demographic_focus_labels) %>%
  summarize(
    n = n(),
    n_big_players = sum(big_player_assets, na.rm = TRUE),
    big_player_rate = n_big_players / n,
    median_assets = median(assets[assets > 0], na.rm = TRUE),
    gini_assets = gini(assets),
    .groups = "drop"
  ) %>%
  filter(n >= 100) %>%
  arrange(desc(big_player_rate))
write_csv(by_demo, file.path(path_final, "inequality_by_demographic_focus.csv"))

if ("support_intersection" %in% names(fdn)) {
  by_construct_intersection <- fdn %>%
    group_by(support_intersection) %>%
    summarize(
      n = n(),
      n_big_players = sum(big_player_assets, na.rm = TRUE),
      big_player_rate = n_big_players / n,
      median_assets = median(assets[assets > 0], na.rm = TRUE),
      gini_assets = gini(assets),
      top1pct_asset_share = top_share(assets, 0.01),
      .groups = "drop"
    ) %>%
    arrange(desc(big_player_rate))
  write_csv(by_construct_intersection, file.path(path_final, "inequality_by_construct_intersection.csv"))

  by_ruca_construct <- fdn %>%
    filter(!is.na(ruca_category)) %>%
    group_by(ruca_category, support_intersection) %>%
    summarize(
      n = n(),
      median_assets = median(assets[assets > 0], na.rm = TRUE),
      gini_assets = gini(assets),
      .groups = "drop"
    )
  write_csv(by_ruca_construct, file.path(path_final, "inequality_by_ruca_x_construct.csv"))
}

if ("taie_profile" %in% names(fdn)) {
  by_taie_profile <- fdn %>%
    group_by(taie_profile) %>%
    summarize(
      n = n(),
      n_big_players = sum(big_player_assets, na.rm = TRUE),
      big_player_rate = n_big_players / n,
      median_assets = median(assets[assets > 0], na.rm = TRUE),
      gini_assets = gini(assets),
      top1pct_asset_share = top_share(assets, 0.01),
      .groups = "drop"
    ) %>%
    arrange(desc(big_player_rate))
  write_csv(by_taie_profile, file.path(path_final, "inequality_by_taie_profile.csv"))

  by_taie_construct <- bind_rows(
    fdn %>% transmute(ein, construct = "tech", flag = coalesce(tech_interest_flag, FALSE), assets, big_player_assets),
    fdn %>% transmute(ein, construct = "ai", flag = coalesce(ai_interest_flag, FALSE), assets, big_player_assets),
    fdn %>% transmute(ein, construct = "innovation", flag = coalesce(innovation_interest_flag, FALSE), assets, big_player_assets),
    fdn %>% transmute(ein, construct = "entrepreneurship", flag = coalesce(entrepreneurship_interest_flag, FALSE), assets, big_player_assets)
  ) %>%
    filter(flag) %>%
    group_by(construct) %>%
    summarize(
      n = n(),
      n_big_players = sum(big_player_assets, na.rm = TRUE),
      big_player_rate = n_big_players / n,
      median_assets = median(assets[assets > 0], na.rm = TRUE),
      gini_assets = gini(assets),
      .groups = "drop"
    ) %>%
    arrange(desc(big_player_rate))
  write_csv(by_taie_construct, file.path(path_final, "inequality_by_taie_construct.csv"))
}

# Publication-friendly grayscale figures.
p_ruca <- by_ruca %>%
  ggplot(aes(x = reorder(ruca_category, big_player_rate), y = big_player_rate)) +
  geom_col(fill = "grey35") +
  coord_flip() +
  scale_y_continuous(labels = percent_format()) +
  labs(x = "RUCA category", y = "Top 1% asset player share",
       title = "Large-player concentration by RUCA category") +
  theme_bw(base_size = 12)
ggsave(file.path(path_figures, "fig_big_player_rate_by_ruca.png"), p_ruca, width = 8, height = 5, dpi = 300)

if (exists("by_issue")) {
  p_issue <- by_issue %>%
    slice_max(order_by = n, n = 12) %>%
    ggplot(aes(x = reorder(issue_focus_primary, gini_assets), y = gini_assets)) +
    geom_col(fill = "grey45") +
    coord_flip() +
    labs(x = "Primary issue focus", y = "Asset Gini",
         title = "Within-focus asset inequality (top 12 issue groups by N)") +
    theme_bw(base_size = 12)
  ggsave(file.path(path_figures, "fig_gini_by_primary_issue.png"), p_issue, width = 8, height = 5, dpi = 300)
}

p_scatter <- fdn %>%
  filter(!is.na(ruca_category), !is.na(assets), assets > 0) %>%
  ggplot(aes(x = ruca_category, y = assets)) +
  geom_boxplot(fill = "grey85", color = "grey20", outlier.alpha = 0.2) +
  scale_y_log10(labels = comma_format()) +
  labs(x = "RUCA category", y = "Assets (log scale)",
       title = "Asset distributions by RUCA category") +
  theme_bw(base_size = 12)
ggsave(file.path(path_figures, "fig_assets_boxplot_by_ruca.png"), p_scatter, width = 8, height = 5, dpi = 300)

if ("support_intersection" %in% names(fdn)) {
  p_construct <- fdn %>%
    count(support_intersection) %>%
    ggplot(aes(x = reorder(support_intersection, n), y = n)) +
    geom_col(fill = "grey40") +
    coord_flip() +
    scale_y_continuous(labels = comma_format()) +
    labs(x = "Construct support group", y = "Foundation count",
         title = "Foundations by democracy/state-capacity support classification") +
    theme_bw(base_size = 12)
  ggsave(file.path(path_figures, "fig_construct_support_distribution.png"), p_construct, width = 8, height = 5, dpi = 300)
}

if ("taie_profile" %in% names(fdn)) {
  p_taie <- fdn %>%
    count(taie_profile) %>%
    ggplot(aes(x = reorder(taie_profile, n), y = n)) +
    geom_col(fill = "grey45") +
    coord_flip() +
    scale_y_continuous(labels = comma_format()) +
    labs(x = "TAIE profile", y = "Foundation count",
         title = "Foundations by tech/AI/innovation/entrepreneurship profile") +
    theme_bw(base_size = 12)
  ggsave(file.path(path_figures, "fig_taie_profile_distribution.png"), p_taie, width = 8, height = 5, dpi = 300)
}

message("[03] Done. Tables in processed_data/final and figures in processed_data/figures.")
