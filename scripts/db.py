"""Lightweight SQLite helpers for listing history and signals."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import sqlite3
from typing import Dict, List, Optional


def _iso(ts: Optional[dt.datetime] = None) -> str:
    ts = ts or dt.datetime.utcnow()
    return ts.replace(microsecond=0).isoformat() + "Z"


def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts:
        return None
    ts = ts.rstrip("Z")
    try:
        return dt.datetime.fromisoformat(ts)
    except ValueError:
        return None


def _normalize_text(val: Optional[str]) -> str:
    if not val:
        return ""
    return " ".join(val.lower().split())


def signature_for(listing: Dict) -> str:
    """Create a property signature for relist detection."""
    parts = [
        _normalize_text(listing.get("address")),
        _normalize_text(listing.get("city")),
        str(listing.get("beds") or ""),
        str(listing.get("baths") or ""),
        str(listing.get("sqft") or ""),
    ]
    raw = "||".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listings_current(
            listing_id TEXT PRIMARY KEY,
            source TEXT,
            url TEXT,
            title TEXT,
            address TEXT,
            city TEXT,
            price INT,
            beds REAL,
            baths REAL,
            sqft INT,
            assessed INT,
            desc_hash TEXT,
            first_seen TEXT,
            last_seen TEXT,
            is_active INT,
            signature TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT,
            event_time TEXT,
            event_type TEXT,
            old_value TEXT,
            new_value TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_presence(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT,
            seen_time TEXT
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_listing ON listing_events(listing_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON listing_events(event_time);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_presence_listing ON listing_presence(listing_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signature ON listings_current(signature);")
    return conn


def upsert_listing_current(conn: sqlite3.Connection, listing: Dict, seen_time: Optional[dt.datetime] = None) -> Dict:
    seen = _iso(seen_time)
    listing_id = listing["listing_id"]
    signature = listing.get("signature") or signature_for(listing)
    desc_hash = hashlib.sha1((listing.get("description") or "").encode("utf-8")).hexdigest()

    cur = conn.cursor()
    cur.execute("SELECT price, is_active FROM listings_current WHERE listing_id=?", (listing_id,))
    row = cur.fetchone()

    old_price = None
    price_changed = False

    if row:
        old_price, _is_active = row
        price_changed = old_price is not None and listing.get("price") is not None and int(old_price) != int(listing["price"])
        cur.execute(
            """
            UPDATE listings_current
            SET source=?, url=?, title=?, address=?, city=?, price=?, beds=?, baths=?, sqft=?,
                assessed=?, desc_hash=?, last_seen=?, is_active=1, signature=?
            WHERE listing_id=?;
            """,
            (
                listing.get("source"),
                listing.get("url"),
                listing.get("title"),
                listing.get("address"),
                listing.get("city"),
                listing.get("price"),
                listing.get("beds"),
                listing.get("baths"),
                listing.get("sqft"),
                listing.get("bc_assessed_value") or listing.get("assessed"),
                desc_hash,
                seen,
                signature,
                listing_id,
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO listings_current(
                listing_id, source, url, title, address, city, price, beds, baths, sqft,
                assessed, desc_hash, first_seen, last_seen, is_active, signature
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?);
            """,
            (
                listing_id,
                listing.get("source"),
                listing.get("url"),
                listing.get("title"),
                listing.get("address"),
                listing.get("city"),
                listing.get("price"),
                listing.get("beds"),
                listing.get("baths"),
                listing.get("sqft"),
                listing.get("bc_assessed_value") or listing.get("assessed"),
                desc_hash,
                seen,
                seen,
                signature,
            ),
        )

    conn.commit()
    return {"price_changed": price_changed, "old_price": old_price, "signature": signature}


def record_event(conn: sqlite3.Connection, listing_id: str, event_type: str, old_value: str, new_value: str, event_time: Optional[dt.datetime] = None) -> None:
    conn.execute(
        "INSERT INTO listing_events(listing_id, event_time, event_type, old_value, new_value) VALUES (?, ?, ?, ?, ?);",
        (listing_id, _iso(event_time), event_type, str(old_value), str(new_value)),
    )
    conn.commit()


def mark_seen(conn: sqlite3.Connection, listing_id: str, seen_time: Optional[dt.datetime] = None) -> None:
    seen = _iso(seen_time)
    conn.execute("INSERT INTO listing_presence(listing_id, seen_time) VALUES (?, ?);", (listing_id, seen))
    conn.execute("UPDATE listings_current SET last_seen=?, is_active=1 WHERE listing_id=?;", (seen, listing_id))
    conn.commit()


def mark_missing(conn: sqlite3.Connection, listing_id: str, missing_time: Optional[dt.datetime] = None) -> None:
    missing = _iso(missing_time)
    conn.execute("UPDATE listings_current SET is_active=0, last_seen=? WHERE listing_id=?;", (missing, listing_id))
    record_event(conn, listing_id, "missing", None, "missing", missing_time)


def get_listing_history(conn: sqlite3.Connection, listing_id: str, days: int = 90) -> List[Dict]:
    cutoff = _iso(dt.datetime.utcnow() - dt.timedelta(days=days))
    cur = conn.cursor()
    cur.execute(
        "SELECT event_time, event_type, old_value, new_value FROM listing_events WHERE listing_id=? AND event_time >= ? ORDER BY event_time ASC;",
        (listing_id, cutoff),
    )
    out = []
    for event_time, event_type, old_value, new_value in cur.fetchall():
        out.append(
            {
                "event_time": event_time,
                "event_type": event_type,
                "old_value": old_value,
                "new_value": new_value,
            }
        )
    return out


def compute_dom_days(conn: sqlite3.Connection, listing_id: str, now: Optional[dt.datetime] = None) -> Optional[int]:
    now = now or dt.datetime.utcnow()
    cur = conn.cursor()
    cur.execute("SELECT first_seen FROM listings_current WHERE listing_id=?;", (listing_id,))
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    first_seen = _parse_iso(row[0])
    if not first_seen:
        return None
    return max(0, (now - first_seen).days)


def compute_price_drop_30d(conn: sqlite3.Connection, listing_id: str, now: Optional[dt.datetime] = None) -> float:
    now = now or dt.datetime.utcnow()
    cur = conn.cursor()
    cur.execute("SELECT price FROM listings_current WHERE listing_id=?;", (listing_id,))
    row = cur.fetchone()
    if not row:
        return 0.0
    current_price = row[0] or 0
    cutoff = now - dt.timedelta(days=30)
    events = get_listing_history(conn, listing_id, days=30)

    prices: List[float] = []
    for ev in events:
        if ev["event_type"] == "price_change":
            try:
                prices.append(float(ev["old_value"]))
                prices.append(float(ev["new_value"]))
            except (TypeError, ValueError):
                continue
    prices.append(float(current_price))

    prices = [p for p in prices if p is not None]
    if not prices:
        return 0.0
    max_price = max(prices)
    if max_price <= 0:
        return 0.0
    drop = (max_price - float(current_price)) / max_price
    return round(max(drop, 0.0), 4)


def detect_relist(conn: sqlite3.Connection, listing_id: str, now: Optional[dt.datetime] = None) -> bool:
    now = now or dt.datetime.utcnow()
    cur = conn.cursor()
    cur.execute("SELECT signature FROM listings_current WHERE listing_id=?;", (listing_id,))
    sig_row = cur.fetchone()
    if not sig_row or not sig_row[0]:
        return False
    signature = sig_row[0]

    cur.execute(
        "SELECT event_time FROM listing_events WHERE listing_id=? AND event_type='missing' ORDER BY event_time DESC LIMIT 1;",
        (listing_id,),
    )
    missing_row = cur.fetchone()
    if missing_row and missing_row[0]:
        missing_time = _parse_iso(missing_row[0])
        if missing_time and (now - missing_time).days >= 7:
            return True

    cur.execute(
        "SELECT listing_id, is_active, last_seen FROM listings_current WHERE signature=? AND listing_id != ? ORDER BY last_seen DESC LIMIT 1;",
        (signature, listing_id),
    )
    other = cur.fetchone()
    if other:
        _other_id, other_active, other_last_seen = other
        if other_active == 0:
            if other_last_seen:
                last_seen_dt = _parse_iso(other_last_seen)
                if last_seen_dt and (now - last_seen_dt).days >= 7:
                    return True
            return True
    return False
