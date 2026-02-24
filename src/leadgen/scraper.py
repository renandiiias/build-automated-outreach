from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass, field

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


@dataclass
class ScrapeRequest:
    audience: str
    location: str
    max_results: int = 60
    headless: bool = True
    min_action_delay_ms: int = 1800
    max_action_delay_ms: int = 4200
    long_pause_every_n: int = 20
    long_pause_min_ms: int = 45000
    long_pause_max_ms: int = 90000
    max_consecutive_errors: int = 3


@dataclass
class ScrapeResult:
    rows: list[dict]
    paused: bool
    pause_reason: str
    captcha_events: int
    timeout_events: int
    http_429_events: int
    consecutive_error_peak: int
    unstable: bool


@dataclass
class ScrapeRuntime:
    captcha_events: int = 0
    timeout_events: int = 0
    http_429_events: int = 0
    consecutive_errors: int = 0
    consecutive_error_peak: int = 0


class ScrapePausedError(RuntimeError):
    pass


class GoogleMapsScraper:
    def scrape(self, req: ScrapeRequest) -> ScrapeResult:
        query = f"{req.audience} em {req.location}"
        runtime = ScrapeRuntime()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=req.headless)
            context = browser.new_context()
            page = context.new_page()
            try:
                page.goto("https://www.google.com/maps", timeout=90000)
                self._random_pause(req)
                self._accept_possible_consent(page)
                self._fill_search_query(page, query)
                page.keyboard.press("Enter")
                self._random_pause(req)
            except PlaywrightTimeoutError:
                runtime.timeout_events += 1
                runtime.consecutive_errors += 1
                runtime.consecutive_error_peak = max(runtime.consecutive_error_peak, runtime.consecutive_errors)
                self._assert_not_paused(req, runtime)
                return ScrapeResult(
                    rows=[],
                    paused=False,
                    pause_reason="",
                    captcha_events=runtime.captcha_events,
                    timeout_events=runtime.timeout_events,
                    http_429_events=runtime.http_429_events,
                    consecutive_error_peak=runtime.consecutive_error_peak,
                    unstable=True,
                )

            self._load_more_results(page, req.max_results, req, runtime)

            links = page.locator('a.hfpxzc')
            total = min(links.count(), req.max_results)
            rows: list[dict] = []

            for idx in range(total):
                self._detect_risk_signals(page, runtime)
                self._assert_not_paused(req, runtime)
                try:
                    link = links.nth(idx)
                    link.click(timeout=20000)
                    self._random_pause(req)
                    rows.append(self._extract_place(page, query))
                    runtime.consecutive_errors = 0
                except PlaywrightTimeoutError:
                    runtime.timeout_events += 1
                    runtime.consecutive_errors += 1
                    runtime.consecutive_error_peak = max(runtime.consecutive_error_peak, runtime.consecutive_errors)
                    continue
                except Exception as exc:
                    msg = str(exc).lower()
                    if "429" in msg:
                        runtime.http_429_events += 1
                    runtime.consecutive_errors += 1
                    runtime.consecutive_error_peak = max(runtime.consecutive_error_peak, runtime.consecutive_errors)
                    continue

                if (idx + 1) % max(1, req.long_pause_every_n) == 0:
                    self._long_pause(req)

            context.close()
            browser.close()

        unique = {}
        for row in rows:
            unique[row.get("maps_url", f"fallback-{time.time_ns()}")] = row

        unstable = runtime.captcha_events > 0 or runtime.timeout_events >= req.max_consecutive_errors or runtime.http_429_events > 0
        return ScrapeResult(
            rows=list(unique.values()),
            paused=False,
            pause_reason="",
            captcha_events=runtime.captcha_events,
            timeout_events=runtime.timeout_events,
            http_429_events=runtime.http_429_events,
            consecutive_error_peak=runtime.consecutive_error_peak,
            unstable=unstable,
        )

    def _fill_search_query(self, page: Page, query: str) -> None:
        selectors = [
            'input#searchboxinput',
            'input[aria-label*="Pesquisar"]',
            'input[aria-label*="Search Google Maps"]',
            'input[placeholder*="Pesquisar"]',
            'input[placeholder*="Search"]',
        ]
        for selector in selectors:
            loc = page.locator(selector).first
            try:
                loc.wait_for(state="visible", timeout=12000)
                loc.fill(query, timeout=15000)
                return
            except Exception:
                continue
        raise PlaywrightTimeoutError("searchbox_not_found")

    def _accept_possible_consent(self, page: Page) -> None:
        consent_labels = [
            "Aceitar tudo",
            "I agree",
            "Accept all",
            "Concordo",
        ]
        for label in consent_labels:
            btn = page.get_by_role("button", name=label).first
            try:
                if btn.count() > 0:
                    btn.click(timeout=2000)
                    time.sleep(1.0)
                    return
            except Exception:
                continue

    def _load_more_results(self, page: Page, max_results: int, req: ScrapeRequest, runtime: ScrapeRuntime) -> None:
        panel = page.locator('div[role="feed"]')
        if panel.count() == 0:
            return

        prev_count = 0
        stable_rounds = 0
        while True:
            self._detect_risk_signals(page, runtime)
            self._assert_not_paused(req, runtime)

            cards = page.locator('a.hfpxzc').count()
            if cards >= max_results:
                return
            if cards == prev_count:
                stable_rounds += 1
            else:
                stable_rounds = 0

            if stable_rounds >= 5:
                return

            prev_count = cards
            panel.evaluate("el => el.scrollBy(0, 2400)")
            self._random_pause(req)

    def _detect_risk_signals(self, page: Page, runtime: ScrapeRuntime) -> None:
        content = page.content().lower()
        if "captcha" in content or "unusual traffic" in content or "detected unusual traffic" in content:
            runtime.captcha_events += 1
            runtime.consecutive_errors += 1
            runtime.consecutive_error_peak = max(runtime.consecutive_error_peak, runtime.consecutive_errors)

    def _assert_not_paused(self, req: ScrapeRequest, runtime: ScrapeRuntime) -> None:
        if runtime.consecutive_errors >= req.max_consecutive_errors:
            reasons = []
            if runtime.captcha_events:
                reasons.append("captcha")
            if runtime.timeout_events:
                reasons.append("timeout")
            if runtime.http_429_events:
                reasons.append("http_429")
            suffix = ",".join(reasons) or "error_streak"
            raise ScrapePausedError(f"SCRAPE_PAUSED:{suffix}")

    def _extract_place(self, page: Page, search_query: str) -> dict:
        name = self._safe_text(page, "h1.DUwDvf")
        rating_raw = self._safe_text(page, "div.F7nice span span")
        rating = self._extract_rating(rating_raw)
        reviews = self._extract_reviews(page.content())

        address = self._safe_text(page, 'button[data-item-id="address"]')
        website = self._safe_attr(page, 'a[data-item-id="authority"]', "href")
        phone = self._safe_text(page, 'button[data-item-id^="phone"]')
        category = self._safe_text(page, 'button[jsaction*="pane.rating.category"]')

        return {
            "search_query": search_query,
            "name": name,
            "category": category,
            "rating": rating,
            "reviews": reviews,
            "phone": phone,
            "website": website,
            "address": address,
            "maps_url": page.url,
        }

    @staticmethod
    def _safe_text(page: Page, selector: str) -> str:
        loc = page.locator(selector)
        if loc.count() == 0:
            return ""
        try:
            return loc.first.inner_text(timeout=1000).strip()
        except Exception:
            return ""

    @staticmethod
    def _safe_attr(page: Page, selector: str, attr: str) -> str:
        loc = page.locator(selector)
        if loc.count() == 0:
            return ""
        try:
            value = loc.first.get_attribute(attr, timeout=1000)
            return (value or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _extract_rating(raw: str) -> str:
        match = re.search(r"\d+[\.,]?\d*", raw or "")
        return match.group(0).replace(",", ".") if match else ""

    @staticmethod
    def _extract_reviews(html: str) -> str:
        match = re.search(r"([\d\.,]+)\s*(avaliac|review)", html, flags=re.IGNORECASE)
        if not match:
            return ""
        return match.group(1).replace(".", "").replace(",", ".")

    @staticmethod
    def _random_pause(req: ScrapeRequest) -> None:
        low = max(100, req.min_action_delay_ms)
        high = max(low, req.max_action_delay_ms)
        time.sleep(random.uniform(low / 1000.0, high / 1000.0))

    @staticmethod
    def _long_pause(req: ScrapeRequest) -> None:
        low = max(500, req.long_pause_min_ms)
        high = max(low, req.long_pause_max_ms)
        time.sleep(random.uniform(low / 1000.0, high / 1000.0))
