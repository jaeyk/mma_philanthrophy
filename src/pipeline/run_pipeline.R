source(file.path("src", "pipeline", "00_config.R"))

scripts <- c(
  file.path("src", "pipeline", "01_build_foundation_universe.R"),
  file.path("src", "pipeline", "01b_scrape_foundation_texts.R"),
  file.path("src", "pipeline", "02_classify_focus.R"),
  file.path("src", "pipeline", "02b_score_democracy_state_capacity.R"),
  file.path("src", "pipeline", "02c_score_taie_constructs.R"),
  file.path("src", "pipeline", "03_analyze_spatial_financial_inequality.R")
)

for (s in scripts) {
  message("Running: ", s)
  source(s)
}

message("Pipeline complete.")
message("Key outputs:")
message("  - ", file.path(path_final, "inequality_overall.csv"))
message("  - ", file.path(path_final, "inequality_by_ruca.csv"))
message("  - ", file.path(path_final, "construct_support_summary.csv"))
message("  - ", file.path(path_final, "taie_support_summary.csv"))
message("  - ", file.path(path_final, "inequality_by_construct_intersection.csv"))
message("  - ", file.path(path_final, "inequality_by_taie_construct.csv"))
message("  - ", file.path(path_final, "inequality_by_primary_issue.csv"))
message("  - ", file.path(path_figures, "fig_big_player_rate_by_ruca.png"))
