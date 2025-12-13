#!/usr/bin/env python3
"""Build deal alerts JSON for a static site (demo or gentle live POC)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from db import (
    compute_dom_days,
    compute_price_drop_30d,
    detect_relist,
    init_db,
    mark_missing,
    mark_seen,
    record_event,
    signature_for,
    upsert_listing_current,
)
from fetch_public import fetch_listings

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "settings.json"
DB_PATH = BASE_DIR / "data" / "app.sqlite"

DEFAULT_SETTINGS = {
    "target_cities": ["Vancouver", "Burnaby", "Richmond"],
    "run_frequency": "daily",
    "limits": {
        "max_search_pages": 2,
        "max_detail_pages": 40,
        "min_delay_seconds": 2.5,
        "max_delay_seconds": 5.0,
    },
    "signals": {
        "below_assessed_ratio": 0.95,
        "price_drop_ratio_30d": 0.05,
        "dom_days": 45,
        "motivated_keywords_en": ["priced to sell", "motivated", "must sell", "bring your offer"],
        "motivated_keywords_zh": ["急售", "诚意卖", "降价", "低于评估"],
    },
}

PROPERTY_TYPES = ["Condo", "Townhouse", "Detached", "1/2 Duplex"]


@dataclass
class Listing:
    listing_id: str
    source: str
    url: str
    title: str
    address: str
    city: str
    price: int
    beds: Optional[float] = None
    baths: Optional[float] = None
    sqft: Optional[int] = None
    description: str = ""
    bc_assessed_value: Optional[int] = None


def now_iso(ts: Optional[dt.datetime] = None) -> str:
    ts = ts or dt.datetime.utcnow()
    return ts.replace(microsecond=0).isoformat() + "Z"


def stable_id(*parts: str) -> str:
    return hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()[:16]


def safe_write_json(path: str, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def deep_merge(base: Dict, override: Dict) -> Dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_settings() -> Dict:
    settings = dict(DEFAULT_SETTINGS)
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            from_file = json.load(f)
        settings = deep_merge(settings, from_file)
    return settings


def generate_demo_listings(settings: Dict, n: int = 200) -> List[Listing]:
    cities = settings.get("target_cities") or DEFAULT_SETTINGS["target_cities"]
    rng = random.Random(dt.datetime.utcnow().date().toordinal())
    out: List[Listing] = []
    for i in range(n):
        city = cities[i % len(cities)]
        ptype = PROPERTY_TYPES[i % len(PROPERTY_TYPES)]
        beds = rng.choice([1, 2, 3, 4, 5])
        baths = rng.choice([1, 1.5, 2, 2.5, 3])
        sqft = rng.randint(450, 3200)

        base_price = {
            "Vancouver": 1100000,
            "Burnaby": 950000,
            "Richmond": 980000,
            "Surrey": 780000,
            "Coquitlam": 820000,
            "North Vancouver": 1050000,
        }.get(city, 900000)

        if ptype == "Condo":
            base_price = int(base_price * 0.72)
        elif ptype == "Townhouse":
            base_price = int(base_price * 0.86)

        drift = rng.uniform(0.78, 1.22)
        price = int(base_price * drift)
        assessed = int(price * rng.uniform(0.90, 1.15))

        desc_parts = ["Bright layout, great location.", "Walkable to transit and shopping.", "Move-in ready."]
        if rng.random() < 0.18:
            desc_parts.append("Priced to sell. Motivated seller!")
        if rng.random() < 0.12:
            desc_parts.append("Bring your offer, must sell.")
        if rng.random() < 0.10:
            desc_parts.append("急售，诚意卖。")

        url = f"https://example.com/listing/{i}"
        listing_id = stable_id("demo", str(i))
        title = f"{beds} bd • {ptype} in {city}"
        address = f"{100 + i} Example St"

        out.append(
            Listing(
                listing_id=listing_id,
                source="demo",
                url=url,
                title=title,
                address=address,
                city=city,
                price=price,
                beds=beds,
                baths=baths,
                sqft=sqft,
                description=" ".join(desc_parts),
                bc_assessed_value=assessed,
            )
        )
    return out


def keyword_hits(text: str, settings: Dict) -> List[str]:
    t = (text or "").lower()
    hits = []
    for k in settings.get("signals", {}).get("motivated_keywords_en", []):
        if k.lower() in t:
            hits.append(k)
    for k in settings.get("signals", {}).get("motivated_keywords_zh", []):
        if k in (text or ""):
            hits.append(k)
    return hits


def evaluate_listing(listing: Dict, dom_days: Optional[int], price_drop_ratio: float, is_relist: bool, hits: List[str], settings: Dict) -> Tuple[float, List[str], Dict]:
    signals_cfg = settings.get("signals", {})
    reasons: List[str] = []
    score = 0.0

    price = listing.get("price") or 0
    assessed = listing.get("bc_assessed_value") or listing.get("assessed")
    ratio_thresh = float(signals_cfg.get("below_assessed_ratio", DEFAULT_SETTINGS["signals"]["below_assessed_ratio"]))
    is_below_assessed = False
    if assessed:
        ratio = price / max(1, assessed)
        gap = max(0.0, 1 - ratio)
        if ratio <= ratio_thresh:
            is_below_assessed = True
            score += gap * 220
            reasons.append(f"低于评估价 {gap*100:.0f}%")

    drop_thresh = float(signals_cfg.get("price_drop_ratio_30d", DEFAULT_SETTINGS["signals"]["price_drop_ratio_30d"]))
    is_price_drop = price_drop_ratio >= drop_thresh
    if is_price_drop:
        score += price_drop_ratio * 140
        reasons.append(f"近30天下降 {price_drop_ratio*100:.0f}%")

    dom_thresh = int(signals_cfg.get("dom_days", DEFAULT_SETTINGS["signals"]["dom_days"]))
    is_long_dom = dom_days is not None and dom_days >= dom_thresh
    if is_long_dom:
        score += min(dom_days / max(1, dom_thresh), 2.0) * 60
        reasons.append(f"挂牌 {dom_days} 天")

    has_keywords = bool(hits)
    if has_keywords:
        score += 20 + min(len(hits), 3) * 6
        reasons.append(f"包含关键词: {', '.join(hits[:3])}")

    if is_relist:
        score += 10
        reasons.append("可能重新挂牌")

    return round(score, 2), reasons, {
        "is_below_assessed": is_below_assessed,
        "is_price_drop": is_price_drop,
        "is_long_dom": is_long_dom,
        "has_motivated_keywords": has_keywords,
    }


def enrich_listings(listings: List[Dict], settings: Dict, conn) -> List[Dict]:
    now = dt.datetime.utcnow()
    seen_ids = set()
    enriched: List[Dict] = []

    cur = conn.cursor()
    cur.execute("SELECT listing_id FROM listings_current WHERE is_active=1;")
    previously_active = {row[0] for row in cur.fetchall()}

    for raw in listings:
        payload = dict(raw)
        payload["signature"] = signature_for(payload)
        result = upsert_listing_current(conn, payload, now)

        if result.get("price_changed"):
            record_event(conn, payload["listing_id"], "price_change", result.get("old_price"), payload.get("price"), now)

        mark_seen(conn, payload["listing_id"], now)

        dom_days = compute_dom_days(conn, payload["listing_id"], now) or 0
        price_drop_ratio = compute_price_drop_30d(conn, payload["listing_id"], now)
        is_relist = detect_relist(conn, payload["listing_id"], now)
        hits = keyword_hits(payload.get("description", ""), settings)

        score, reasons, signal_flags = evaluate_listing(payload, dom_days, price_drop_ratio, is_relist, hits, settings)
        enriched.append(
            {
                **payload,
                "dom_days": dom_days,
                "price_drop_30d_ratio": price_drop_ratio,
                "is_relist": bool(is_relist),
                "reasons": reasons,
                "score": score,
                **signal_flags,
            }
        )
        seen_ids.add(payload["listing_id"])

    missing_ids = previously_active - seen_ids
    for mid in missing_ids:
        mark_missing(conn, mid, now)

    return enriched


def build_outputs(listings: List[Dict], top_k: int = 50):
    deals = []
    alerts = []

    scored = sorted(listings, key=lambda x: x.get("score", 0), reverse=True)

    for x in scored[:top_k]:
        deals.append(
            {
                "listing_id": x.get("listing_id"),
                "source": x.get("source"),
                "url": x.get("url"),
                "title": x.get("title"),
                "address": x.get("address"),
                "city": x.get("city"),
                "price": x.get("price"),
                "beds": x.get("beds"),
                "baths": x.get("baths"),
                "sqft": x.get("sqft"),
                "bc_assessed_value": x.get("bc_assessed_value"),
                "dom_days": x.get("dom_days"),
                "price_drop_30d_ratio": x.get("price_drop_30d_ratio"),
                "is_relist": x.get("is_relist"),
                "score": x.get("score"),
                "reasons": x.get("reasons", []),
            }
        )

    for x in scored[:10]:
        alerts.append(
            {
                "listing_id": x.get("listing_id"),
                "title": x.get("title"),
                "city": x.get("city"),
                "price": x.get("price"),
                "url": x.get("url"),
                "score": x.get("score"),
                "reasons": x.get("reasons", []),
            }
        )

    return alerts, deals


def fetch_live(source: str, settings: Dict) -> List[Listing]:
    try:
        fetched = fetch_listings(source, settings)
        if fetched:
            return [Listing(**f) if not isinstance(f, Listing) else f for f in fetched]
    except Exception as exc:
        print(f"[live] Failed live fetch: {exc}")
    return []


def to_dict_list(listings: List[Listing]) -> List[Dict]:
    out: List[Dict] = []
    for l in listings:
        out.append(asdict(l) if isinstance(l, Listing) else dict(l))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["demo", "live"], default="demo")
    ap.add_argument("--source", default="public_demo", help="Source for live mode (e.g., realtor_public_poc)")
    ap.add_argument("--top-k", type=int, default=50)
    args = ap.parse_args()

    settings = load_settings()
    conn = init_db(str(DB_PATH))

    if args.mode == "demo":
        listings = generate_demo_listings(settings)
    else:
        listings = fetch_live(args.source, settings)
        if args.source == "public_demo" and not listings:
            listings = generate_demo_listings(settings, n=120)

    enriched = enrich_listings(to_dict_list(listings), settings, conn)
    alerts, deals = build_outputs(enriched, top_k=args.top_k)

    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "public"))
    os.makedirs(out_dir, exist_ok=True)

    safe_write_json(os.path.join(out_dir, "alerts.json"), {"generated_at": now_iso(), "alerts": alerts})
    safe_write_json(os.path.join(out_dir, "top_deals.json"), {"generated_at": now_iso(), "deals": deals})
    safe_write_json(os.path.join(out_dir, "last_run.json"), {
        "generated_at": now_iso(),
        "mode": args.mode,
        "listing_count": len(listings),
        "alert_count": len(alerts),
        "top_count": len(deals),
        "run_frequency": settings.get("run_frequency"),
    })

    print("Wrote JSON to:", out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
