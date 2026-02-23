from __future__ import annotations

import re
import time
from dataclasses import dataclass

from playwright.sync_api import Page, sync_playwright


@dataclass
class ScrapeRequest:
    audience: str
    location: str
    max_results: int = 50
    headless: bool = True


class GoogleMapsScraper:
    def scrape(self, req: ScrapeRequest) -> list[dict]:
        query = f"{req.audience} em {req.location}"
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=req.headless)
            page = browser.new_page()
            page.goto("https://www.google.com/maps", timeout=90000)
            page.wait_for_timeout(2000)

            search_box = page.locator('input#searchboxinput')
            search_box.fill(query)
            page.keyboard.press("Enter")
            page.wait_for_timeout(5000)

            self._load_more_results(page, req.max_results)

            links = page.locator('a.hfpxzc')
            total = min(links.count(), req.max_results)
            rows: list[dict] = []

            for idx in range(total):
                try:
                    link = links.nth(idx)
                    link.click(timeout=20000)
                    page.wait_for_timeout(2500)
                    rows.append(self._extract_place(page, query))
                except Exception:
                    continue

            browser.close()
            # remove duplicates by maps_url
            unique = {}
            for row in rows:
                unique[row.get("maps_url", f"fallback-{time.time_ns()}")] = row
            return list(unique.values())

    def _load_more_results(self, page: Page, max_results: int) -> None:
        panel = page.locator('div[role="feed"]')
        if panel.count() == 0:
            return

        prev_count = 0
        stable_rounds = 0
        while True:
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
            page.wait_for_timeout(1200)

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
        # Matches patterns like "1.234 avaliações" or "123 reviews"
        match = re.search(r"([\d\.,]+)\s*(avaliac|review)", html, flags=re.IGNORECASE)
        if not match:
            return ""
        return match.group(1).replace(".", "").replace(",", ".")
