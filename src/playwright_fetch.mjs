#!/usr/bin/env node

const [,, url, timeoutSecArg = '25', waitMsArg = '700'] = process.argv;

function emit(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

if (!url) {
  emit({
    ok: false,
    error_type: 'missing_url',
    error_message: 'Usage: node src/playwright_fetch.mjs <url> [timeout_seconds] [wait_ms]'
  });
  process.exit(1);
}

const timeoutMs = Number(timeoutSecArg) * 1000;
const waitMs = Number(waitMsArg);

(async () => {
  let chromium;
  try {
    ({ chromium } = await import('playwright'));
  } catch (err) {
    emit({
      ok: false,
      final_url: url,
      status_code: null,
      title: '',
      text_clean: '',
      error_type: 'playwright_not_installed',
      error_message: String(err && err.message ? err.message : err)
    });
    process.exit(0);
  }

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    const page = await context.newPage();

    const response = await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: timeoutMs
    });

    if (waitMs > 0) {
      await page.waitForTimeout(waitMs);
    }

    const [title, textRaw] = await Promise.all([
      page.title().catch(() => ''),
      page.locator('body').innerText().catch(() => '')
    ]);

    const textClean = String(textRaw || '').replace(/\s+/g, ' ').trim();

    emit({
      ok: textClean.length > 0,
      final_url: page.url(),
      status_code: response ? response.status() : null,
      title: String(title || ''),
      text_clean: textClean,
      error_type: textClean.length > 0 ? '' : 'empty_text',
      error_message: textClean.length > 0 ? '' : 'Browser rendered page but no body text extracted'
    });
  } catch (err) {
    emit({
      ok: false,
      final_url: url,
      status_code: null,
      title: '',
      text_clean: '',
      error_type: 'browser_request_error',
      error_message: String(err && err.message ? err.message : err)
    });
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
    }
  }
})();
