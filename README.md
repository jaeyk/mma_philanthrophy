# Foundation Focus Pipeline

This project builds a foundation-level analytic dataset from IRS + URL metadata, enriches it with scraped website text, applies rule-based focus/construct scoring, and produces publication-ready inequality tables and figures.

## What This Pipeline Produces

1. A cleaned foundation universe with URL quality flags and RUCA joins.
2. A reusable web-text corpus (`foundation_web_texts.csv.gz`) plus scrape diagnostics.
3. Weak-supervision labels for issue, geography, and demographic focus.
4. Construct scores for democracy/state capacity and TAIE (tech/AI/innovation/entrepreneurship).
5. Final inequality summaries and figures in `processed_data/final` and `processed_data/figures`.

## Project Layout

- `raw_data/`: input files (IRS and URL data).
- `src/`: pipeline scripts and taxonomies.
- `processed_data/intermediate/`: stage outputs consumed by later stages.
- `processed_data/final/`: final summary tables.
- `processed_data/figures/`: exported PNG figures.

## Required Inputs

Place these files in `raw_data/`:

- `predictions.csv`
- `irs_mbf.csv`
- `irs_to_fips.csv`
- `irs_urls.csv`
- `irs_urls_websites.csv`

The USDA RUCA ZIP file is downloaded automatically and cached to:

- `processed_data/intermediate/usda_ruca_zip_2020.csv`

## Dependencies

R packages used by the pipeline include:

- `readr`
- `dplyr`
- `stringr`
- `tidyr`
- `ggplot2`
- `scales`
- `httr2`
- `rvest`
- `xml2`
- `jsonlite` (needed only when browser fallback is enabled)

Optional browser fallback dependencies (for JS-heavy sites):

```bash
npm install playwright
npx playwright install chromium
```

## Run Modes

### Full pipeline

```bash
Rscript src/run_pipeline.R
```

### Scraper only

```bash
Rscript src/01b_scrape_foundation_texts.R
```

### Hybrid scraper (HTTP first, browser fallback if needed)

```bash
SCRAPER_WORKERS=6 BROWSER_FALLBACK_ENABLED=1 Rscript src/01b_scrape_foundation_texts.R
```

## Pipeline Stages

1. `src/01_build_foundation_universe.R`
- Filters `predictions.csv` to predicted foundations.
- Joins MBF, FIPS, URL metadata, and RUCA.
- Builds `candidate_url` priority logic and `url_quality_keep` flag.
- Writes:
- `processed_data/intermediate/foundation_universe.csv.gz`
- `processed_data/final/foundation_universe_summary.csv`

2. `src/01b_scrape_foundation_texts.R`
- Crawls candidate URLs and key subpaths (`/about`, `/mission`, etc.).
- Extracts page title/body text.
- Uses retry logic and domain-level heuristics.
- Shows progress bar and ETA while running.
- Optional Playwright fallback for JS-rendered/thin pages.
- Writes:
- `processed_data/intermediate/foundation_web_texts.csv.gz`
- `processed_data/final/foundation_web_text_failures.csv`
- `processed_data/final/foundation_web_scrape_audit.csv`
- `processed_data/final/foundation_web_texts_summary.csv`

3. `src/01c_diagnose_scrape_failures.R`
- Samples failed domains.
- Probes HTTP/HTTPS root variants.
- Recommends per-domain handling (`skip_domain`, `prefer_https`, `normal`).
- Writes:
- `processed_data/final/foundation_web_failure_diagnostic_sample.csv`
- `processed_data/final/foundation_web_failure_diagnostic_summary.csv`
- `processed_data/final/foundation_web_failure_domain_recommendations.csv`

4. `src/02_classify_focus.R`
- Builds signal text from metadata + scraped web text.
- Scores issue focus via weighted keyword taxonomy.
- Assigns geographic and demographic weak labels.
- Writes:
- `processed_data/intermediate/foundation_focus_classified.csv.gz`
- `processed_data/final/focus_issue_primary_distribution.csv`
- `processed_data/final/focus_geo_distribution.csv`
- `processed_data/final/focus_demographic_distribution_top25.csv`

5. `src/02b_score_democracy_state_capacity.R`
- Scores democracy/state-capacity constructs from taxonomy matches.
- Builds confidence/flag fields and intersection labels.
- Writes:
- `processed_data/intermediate/foundation_construct_scores.csv.gz`
- `processed_data/final/construct_support_summary.csv`
- `processed_data/final/construct_support_intersection_distribution.csv`

6. `src/02c_score_taie_constructs.R`
- Scores tech/AI/innovation/entrepreneurship constructs.
- Builds TAIE flags and profile labels.
- Writes:
- `processed_data/intermediate/foundation_taie_scores.csv.gz`
- `processed_data/final/taie_support_summary.csv`
- `processed_data/final/taie_profile_distribution.csv`
- `processed_data/final/taie_construct_distribution.csv`

7. `src/03_analyze_spatial_financial_inequality.R`
- Computes inequality and concentration metrics overall and by groups.
- Exports publication-friendly grayscale figures.
- Writes final outputs such as:
- `processed_data/final/inequality_overall.csv`
- `processed_data/final/inequality_by_ruca.csv`
- `processed_data/final/inequality_by_city_tier.csv`
- `processed_data/final/inequality_by_primary_issue.csv`
- `processed_data/final/inequality_by_geo_focus.csv`
- `processed_data/final/inequality_by_demographic_focus.csv`
- `processed_data/final/inequality_by_construct_intersection.csv`
- `processed_data/final/inequality_by_ruca_x_construct.csv`
- `processed_data/final/inequality_by_taie_profile.csv`
- `processed_data/final/inequality_by_taie_construct.csv`
- `processed_data/figures/*.png`

## Scraper Controls (Environment Variables)

Core controls:

- `URL_FILTER_MODE` (default `candidate`): `candidate` or `quality_keep`.
- `MAX_ORGS` (default `ALL`): cap number of foundations.
- `MAX_PAGES_PER_SITE` (default `4`): max candidate URLs per foundation.
- `REQUEST_TIMEOUT_SECONDS` (default `20`): per-request timeout.
- `CRAWL_DELAY_SECONDS` (default `0.4`): delay between attempts.
- `MAX_RETRIES` (default `2`): retries per URL.
- `MIN_TEXT_CHARS_STRONG_SUCCESS` (default `600`): stop early when strong page captured.
- `CONTINUE_IF_THIN_SUCCESS` (default `1`): continue probing when first success is short.
- `SCRAPER_VERBOSE` (default `1`): verbose URL-level logs.
- `SCRAPER_WORKERS` (default `1`): parallel workers target.

Browser fallback controls:

- `BROWSER_FALLBACK_ENABLED` (default `0`): enable Playwright fallback tier.
- `BROWSER_FALLBACK_SCRIPT` (default `src/playwright_fetch.mjs`): node helper path.
- `BROWSER_FALLBACK_TIMEOUT_SECONDS` (default `25`): browser navigation timeout.
- `BROWSER_FALLBACK_WAIT_MS` (default `700`): wait after DOM load.
- `BROWSER_FALLBACK_MIN_TEXT_CHARS` (default `350`): threshold for “thin” pages.

Example tuned run:

```bash
URL_FILTER_MODE=quality_keep \
SCRAPER_WORKERS=6 \
MAX_ORGS=5000 \
MAX_PAGES_PER_SITE=4 \
REQUEST_TIMEOUT_SECONDS=15 \
BROWSER_FALLBACK_ENABLED=1 \
Rscript src/01b_scrape_foundation_texts.R
```

## Taxonomy and Label Files

- `src/issue_focus_keywords.csv`
- `src/democracy_state_capacity_keywords.csv`
- `src/tech_ai_innovation_entrepreneurship_keywords.csv`
- `src/manual_labels_democracy_state_capacity_template.csv`

## Notes on Model/Measurement Quality

- Focus and construct labels are weak-supervision baselines.
- They are useful for scalable first-pass analysis, not final causal claims.
- For publication-quality inference, add manual validation and threshold sensitivity checks.

## Troubleshooting

1. Scraper says browser fallback disabled because `node` is missing.
- Install Node.js and Playwright, or run with `BROWSER_FALLBACK_ENABLED=0`.

2. Scraper appears slow.
- Increase `SCRAPER_WORKERS`.
- Lower `MAX_PAGES_PER_SITE`.
- Use `URL_FILTER_MODE=quality_keep`.
- Keep browser fallback off unless needed.

3. Parallel workers unavailable in constrained environments.
- The scraper automatically falls back to sequential mode and continues.

4. Missing taxonomy or input file errors.
- Confirm all required files exist in `raw_data/` and `src/` with exact names.
