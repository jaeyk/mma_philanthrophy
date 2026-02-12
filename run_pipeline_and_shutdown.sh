#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

LOG_DIR="${ROOT_DIR}/processed_data/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/pipeline_$(date +%Y%m%d_%H%M%S).log"
STATE_FILE="${LOG_DIR}/pipeline_state.env"
COMPLETED_FILE="${LOG_DIR}/pipeline_completed_stages.txt"

VERBOSE="${VERBOSE:-1}"
if [[ "${VERBOSE}" == "1" ]]; then
  set -x
fi

# Scraper runtime controls (override when running the script).
export MAX_ORGS="${MAX_ORGS:-ALL}"
export MAX_PAGES_PER_SITE="${MAX_PAGES_PER_SITE:-4}"
export REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-20}"
export CRAWL_DELAY_SECONDS="${CRAWL_DELAY_SECONDS:-0.4}"
export MAX_RETRIES="${MAX_RETRIES:-2}"
export URL_FILTER_MODE="${URL_FILTER_MODE:-candidate}"
export SCRAPER_WORKERS="${SCRAPER_WORKERS:-6}"
export SCRAPER_RESUME="${SCRAPER_RESUME:-1}"
export SCRAPER_CHECKPOINT_EVERY="${SCRAPER_CHECKPOINT_EVERY:-100}"
export BROWSER_FALLBACK_ENABLED="${BROWSER_FALLBACK_ENABLED:-0}"
export BROWSER_FALLBACK_TIMEOUT_SECONDS="${BROWSER_FALLBACK_TIMEOUT_SECONDS:-25}"
export BROWSER_FALLBACK_WAIT_MS="${BROWSER_FALLBACK_WAIT_MS:-700}"
export BROWSER_FALLBACK_MIN_TEXT_CHARS="${BROWSER_FALLBACK_MIN_TEXT_CHARS:-350}"

# Resume controls:
# - RESUME=1 (default): skip already completed stages from prior run state.
# - RESET_STATE=1: ignore old state and start from scratch.
RESUME="${RESUME:-1}"
RESET_STATE="${RESET_STATE:-0}"

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
  "src/01_build_foundation_universe.R|Scraping/Universe Build"
  "src/01b_scrape_foundation_texts.R|Website Text Scraping"
  "src/01c_diagnose_scrape_failures.R|Scrape Failure Diagnostics"
  "src/02_classify_focus.R|Core Focus Classification"
  "src/02b_score_democracy_state_capacity.R|Democracy/State Capacity Classification"
  "src/02c_score_taie_constructs.R|Tech/AI/Innovation/Entrepreneurship Classification"
  "src/03_analyze_spatial_financial_inequality.R|Spatial-Financial Inequality Analysis"
)

TOTAL="${#STAGES[@]}"
CURRENT=0
START_INDEX=1

if [[ "${RESET_STATE}" == "1" ]]; then
  rm -f "${STATE_FILE}" "${COMPLETED_FILE}"
fi

if [[ "${RESUME}" == "1" && -f "${STATE_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
  if [[ -n "${LAST_COMPLETED_INDEX:-}" ]]; then
    START_INDEX=$((LAST_COMPLETED_INDEX + 1))
  fi
fi

if [[ ! -f "${COMPLETED_FILE}" ]]; then
  : > "${COMPLETED_FILE}"
fi

echo "Pipeline log: ${LOG_FILE}"
echo "Starting pipeline at $(date)" | tee -a "${LOG_FILE}"
echo "Scrape settings: MAX_ORGS=${MAX_ORGS}, MAX_PAGES_PER_SITE=${MAX_PAGES_PER_SITE}, REQUEST_TIMEOUT_SECONDS=${REQUEST_TIMEOUT_SECONDS}, CRAWL_DELAY_SECONDS=${CRAWL_DELAY_SECONDS}, MAX_RETRIES=${MAX_RETRIES}, URL_FILTER_MODE=${URL_FILTER_MODE}, SCRAPER_WORKERS=${SCRAPER_WORKERS}, SCRAPER_RESUME=${SCRAPER_RESUME}, SCRAPER_CHECKPOINT_EVERY=${SCRAPER_CHECKPOINT_EVERY}, BROWSER_FALLBACK_ENABLED=${BROWSER_FALLBACK_ENABLED}, BROWSER_FALLBACK_TIMEOUT_SECONDS=${BROWSER_FALLBACK_TIMEOUT_SECONDS}, BROWSER_FALLBACK_WAIT_MS=${BROWSER_FALLBACK_WAIT_MS}, BROWSER_FALLBACK_MIN_TEXT_CHARS=${BROWSER_FALLBACK_MIN_TEXT_CHARS}" | tee -a "${LOG_FILE}"
echo "Resume settings: RESUME=${RESUME}, RESET_STATE=${RESET_STATE}, START_INDEX=${START_INDEX}" | tee -a "${LOG_FILE}"

if (( START_INDEX > TOTAL )); then
  echo "All stages were already completed in a previous run. Nothing to do." | tee -a "${LOG_FILE}"
else
  for idx in "${!STAGES[@]}"; do
    stage_num=$((idx + 1))
    stage="${STAGES[$idx]}"
    script="${stage%%|*}"
    label="${stage##*|}"

    if (( stage_num < START_INDEX )); then
      continue
    fi

    CURRENT="${stage_num}"
    draw_progress "${CURRENT}" "${TOTAL}" "${label}" | tee -a "${LOG_FILE}"

    echo "Running ${script}..." | tee -a "${LOG_FILE}"
    Rscript "${script}" 2>&1 | tee -a "${LOG_FILE}"

    echo "${stage_num}|${script}|${label}|$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${COMPLETED_FILE}"
    {
      echo "LAST_COMPLETED_INDEX=${stage_num}"
      echo "LAST_COMPLETED_SCRIPT=${script}"
      echo "LAST_COMPLETED_LABEL=\"${label}\""
      echo "LAST_COMPLETED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "${STATE_FILE}"
  done
fi

echo "Pipeline completed successfully at $(date)" | tee -a "${LOG_FILE}"
draw_progress "${TOTAL}" "${TOTAL}" "Done" | tee -a "${LOG_FILE}"

# Mark full completion for future resumptions.
{
  echo "LAST_COMPLETED_INDEX=${TOTAL}"
  echo "LAST_COMPLETED_SCRIPT=ALL"
  echo "LAST_COMPLETED_LABEL=\"Done\""
  echo "LAST_COMPLETED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${STATE_FILE}"

echo "Shutting down..."
# macOS/Linux shutdown; may prompt for sudo password.
sudo shutdown -h now
