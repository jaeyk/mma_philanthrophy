# Foundation Focus Pipeline

This pipeline builds a foundation-level analytic dataset and outputs tables/figures for analyzing the intersection of spatial and financial inequality in the philanthropic sector.

## Scripts

1. `01_build_foundation_universe.R`
- Filters to predicted foundations.
- Joins IRS master fields, FIPS, USDA RUCA ZIP categories, and URL metadata.
- Applies URL quality screen.

2. `01b_scrape_foundation_texts.R`
- Scrapes reusable text corpus from quality-screened foundation websites.
- Saves per-page text and metadata for reuse across classifiers.
- Logs URL failures/errors and continues to next URLs/organizations.

3. `02_classify_focus.R`
- Adds weakly supervised issue/geo/demographic labels from available metadata + scraped web text.
- Issue focus is derived from `src/pipeline/taxonomies/issue_focus_keywords.csv` (not from prediction score ranks).

4. `02b_score_democracy_state_capacity.R`
- Scores two publication-target constructs using keyword taxonomy:
  - `democracy_support`
  - `state_capacity_support`
- Produces intersection labels (`both`, `democracy_only`, `state_capacity_only`, `neither_or_unclear`).

5. `02c_score_taie_constructs.R`
- Scores four additional constructs:
  - `tech_interest`
  - `ai_interest`
  - `innovation_interest`
  - `entrepreneurship_interest`
- Produces profile labels by number of supported constructs (`none` to `all_four`).

6. `03_analyze_spatial_financial_inequality.R`
- Computes inequality and concentration metrics overall and by RUCA/city tier/focus labels.
- Exports publication-friendly grayscale figures.

7. `run_pipeline.R`
- Runs the three scripts in sequence.

## Run

From project root:

```bash
Rscript src/pipeline/run_pipeline.R
```

## Outputs

- `processed_data/intermediate/foundation_universe.csv.gz`
- `processed_data/intermediate/foundation_web_texts.csv.gz`
- `processed_data/final/foundation_web_text_failures.csv`
- `processed_data/intermediate/foundation_focus_classified.csv.gz`
- `processed_data/intermediate/foundation_construct_scores.csv.gz`
- `processed_data/intermediate/foundation_taie_scores.csv.gz`
- `processed_data/final/*.csv`
- `processed_data/figures/*.png`

## Taxonomy + Labeling

- `src/pipeline/taxonomies/democracy_state_capacity_keywords.csv`
- `src/pipeline/taxonomies/issue_focus_keywords.csv`
- `src/pipeline/taxonomies/tech_ai_innovation_entrepreneurship_keywords.csv`
- `src/pipeline/templates/manual_labels_democracy_state_capacity_template.csv`

## Notes

- Geographic and demographic focus classifiers are weak-supervision baselines using current available fields. They are designed as a starting point and should be upgraded with richer scraped text and a manually labeled validation set for publication claims.
- Democracy/state-capacity scores are rule-based baseline signals designed for transparent first-pass measurement. Use manual labels to calibrate thresholds and report sensitivity analyses.
