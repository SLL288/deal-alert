"""Gentle, cache-first fetching helpers.

Do not crawl aggressively. Use compliant sources or partner feeds for production.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import random
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

DEFAULT_USER_AGENT = "DealRadarMVP/0.1 (+https://example.com/)"
CACHE_HOURS = 12


def throttle(min_s: float, max_s: float) -> None:
    delay = random.uniform(min_s, max_s)
    time.sleep(delay)


def fetch_robots_txt(base_url: str) -> Optional[RobotFileParser]:
    rp = RobotFileParser()
    rp.set_url(urljoin(base_url, "/robots.txt"))
    try:
        rp.read()
        return rp
    except Exception:
        return None


def is_allowed(user_agent: str, url: str, robots: Optional[RobotFileParser]) -> bool:
    if robots is None:
        return False
    return robots.can_fetch(user_agent, url)


def _cache_path(url: str, cache_dir: str) -> str:
    fname = hashlib.sha1(url.encode("utf-8")).hexdigest() + ".cache"
    return os.path.join(cache_dir, fname)


def http_get(url: str, headers: Optional[Dict] = None, timeout: int = 12, cache_dir: str = "cache", min_delay: float = 2.5, max_delay: float = 5.0) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    path = _cache_path(url, cache_dir)
    now = time.time()
    if os.path.exists(path) and (now - os.path.getmtime(path)) < CACHE_HOURS * 3600:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    throttle(min_delay, max_delay)
    resp = requests.get(url, headers=headers or {"User-Agent": DEFAULT_USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    with open(path, "w", encoding="utf-8") as f:
        f.write(resp.text)
    return resp.text


def _parse_listings_from_search(html: str) -> List[Dict]:
    """Very small extractor for proof-of-concept."""
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict] = []
    for card in soup.select("[data-listingid]"):
        try:
            listing_id = card.get("data-listingid")
            title = card.get_text(" ", strip=True)[:140]
            price_el = card.select_one(".price, [data-price]")
            price_text = price_el.get_text(strip=True) if price_el else ""
            price = None
            if price_text:
                digits = "".join(ch for ch in price_text if ch.isdigit())
                if digits:
                    price = int(digits)
            url_el = card.select_one("a[href]")
            href = url_el["href"] if url_el else ""
            out.append(
                {
                    "listing_id": listing_id or hashlib.sha1(title.encode("utf-8")).hexdigest()[:12],
                    "source": "realtor_public_poc",
                    "url": href,
                    "title": title or "Listing",
                    "address": "",
                    "city": "",
                    "price": price or 0,
                    "description": title or "",
                }
            )
        except Exception:
            continue
    return out


def fetch_realtor_public_poc(settings: Dict) -> List[Dict]:
    base_url = "https://www.realtor.ca"
    robots = fetch_robots_txt(base_url)
    if robots is None:
        print("[fetch] Could not read robots.txt, skipping crawl for safety.")
        return []

    sample_path = "/"
    if not is_allowed(DEFAULT_USER_AGENT, sample_path, robots):
        print("[fetch] robots.txt disallows fetch, exiting live crawl.")
        return []

    limits = settings.get("limits", {})
    max_search_pages = int(limits.get("max_search_pages", 2))
    max_detail_pages = int(limits.get("max_detail_pages", 40))
    min_delay = float(limits.get("min_delay_seconds", 2.5))
    max_delay = float(limits.get("max_delay_seconds", 5.0))

    listings: List[Dict] = []
    detail_count = 0
    cache_dir = os.path.join(os.path.dirname(__file__), "..", "cache")

    for city in settings.get("target_cities", [])[: max_search_pages]:
        search_url = f"{base_url}/map#City={city}"
        if not is_allowed(DEFAULT_USER_AGENT, search_url, robots):
            print(f"[fetch] robots.txt disallows {search_url}, skipping.")
            continue
        try:
            html = http_get(search_url, cache_dir=cache_dir, min_delay=min_delay, max_delay=max_delay)
            parsed = _parse_listings_from_search(html)
            listings.extend(parsed)
        except Exception as exc:
            print(f"[fetch] Failed search page for {city}: {exc}")

    # Limit detail lookups (placeholder, since public pages may block)
    limited = []
    for listing in listings:
        if detail_count >= max_detail_pages:
            break
        limited.append(listing)
        detail_count += 1

    return limited


def fetch_listings(source: str, settings: Dict) -> List[Dict]:
    if source == "public_demo":
        return []
    if source == "realtor_public_poc":
        return fetch_realtor_public_poc(settings)
    raise ValueError(f"Unknown source: {source}")
