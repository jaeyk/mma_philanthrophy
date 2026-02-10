#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

LOG_DIR="${ROOT_DIR}/processed_data/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/pipeline_$(date +%Y%m%d_%H%M%S).log"

VERBOSE="${VERBOSE:-1}"
if [[ "${VERBOSE}" == "1" ]]; then
  set -x
fi

# Scraper runtime controls (override when running the script).
export MAX_ORGS="${MAX_ORGS:-5000}"
export MAX_PAGES_PER_SITE="${MAX_PAGES_PER_SITE:-4}"
export REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-20}"
export CRAWL_DELAY_SECONDS="${CRAWL_DELAY_SECONDS:-0.4}"
export MAX_RETRIES="${MAX_RETRIES:-2}"

draw_progress() {
  local current="$1"
  local total="$2"
  local label="$3"
  local width=30
  local filled=$(( current * width / total ))
  local empty=$(( width - filled ))
  local bar
  bar="$(printf '%*s' "${filled}" '' | tr ' ' '#')$(printf '%*s' "${empty}" '' | tr ' ' '-')"
  local pct=$(( current * 100 / total ))
  printf "\n[%s] %3d%% (%d/%d) %s\n" "${bar}" "${pct}" "${current}" "${total}" "${label}"
}

STAGES=(
  "src/pipeline/01_build_foundation_universe.R|Scraping/Universe Build"
  "src/pipeline/01b_scrape_foundation_texts.R|Website Text Scraping"
  "src/pipeline/02_classify_focus.R|Core Focus Classification"
  "src/pipeline/02b_score_democracy_state_capacity.R|Democracy/State Capacity Classification"
  "src/pipeline/02c_score_taie_constructs.R|Tech/AI/Innovation/Entrepreneurship Classification"
  "src/pipeline/03_analyze_spatial_financial_inequality.R|Spatial-Financial Inequality Analysis"
)

TOTAL="${#STAGES[@]}"
CURRENT=0

echo "Pipeline log: ${LOG_FILE}"
echo "Starting pipeline at $(date)" | tee -a "${LOG_FILE}"
echo "Scrape settings: MAX_ORGS=${MAX_ORGS}, MAX_PAGES_PER_SITE=${MAX_PAGES_PER_SITE}, REQUEST_TIMEOUT_SECONDS=${REQUEST_TIMEOUT_SECONDS}, CRAWL_DELAY_SECONDS=${CRAWL_DELAY_SECONDS}, MAX_RETRIES=${MAX_RETRIES}" | tee -a "${LOG_FILE}"

for stage in "${STAGES[@]}"; do
  script="${stage%%|*}"
  label="${stage##*|}"
  CURRENT=$((CURRENT + 1))
  draw_progress "${CURRENT}" "${TOTAL}" "${label}" | tee -a "${LOG_FILE}"

  echo "Running ${script}..." | tee -a "${LOG_FILE}"
  Rscript "${script}" 2>&1 | tee -a "${LOG_FILE}"
done

echo "Pipeline completed successfully at $(date)" | tee -a "${LOG_FILE}"
draw_progress "${TOTAL}" "${TOTAL}" "Done" | tee -a "${LOG_FILE}"

echo "Shutting down..."
# macOS/Linux shutdown; may prompt for sudo password.
sudo shutdown -h now
