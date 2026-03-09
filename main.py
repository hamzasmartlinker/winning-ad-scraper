"""\nWinning Ad Scraper â FastAPI webhook service\nScrapes Meta Ad Library & TikTok for winning ads via Apify,\nstores results in local SQLite + Supabase.\n"""

import os
import re
import json
import time
import sqlite3
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "winning_ads")
COUNTRY_CODE = os.getenv("COUNTRY_CODE", "DZ")
MIN_RUN_DAYS = int(os.getenv("MIN_RUN_DAYS", "7"))
ACTIVE_DAYS_LIMIT = int(os.getenv("ACTIVE_DAYS_LIMIT", "30"))
DB_PATH = os.getenv("DB_PATH", "data/winning_ads.db")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("winning-ad-scraper")

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS winning_ads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name    TEXT NOT NULL,
            platform        TEXT NOT NULL,
            brand_name      TEXT,
            ad_text         TEXT,
            video_url       TEXT,
            creative_url    TEXT,
            start_date      TEXT,
            run_duration_days INTEGER,
            ad_id           TEXT,
            raw_metadata    TEXT,
            price           TEXT,
            landing_page_url TEXT,
            direct_video_url TEXT,
            direct_ad_url   TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def _insert_local(conn: sqlite3.Connection, rows: list[dict]):
    for r in rows:
        conn.execute(
            """INSERT INTO winning_ads
               (product_name, platform, brand_name, ad_text, video_url,
                creative_url, start_date, run_duration_days, ad_id, raw_metadata,
                price, landing_page_url, direct_video_url, direct_ad_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                r["product_name"], r["platform"], r["brand_name"],
                r["ad_text"], r["video_url"], r["creative_url"],
                r["start_date"], r["run_duration_days"], r["ad_id"],
                r["raw_metadata"],
                r.get("price"), r.get("landing_page_url"), r.get("direct_video_url"), r.get("direct_ad_url"),
            ),
        )
    conn.commit()
    logger.info("Inserted %d rows into local SQLite", len(rows))

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

async def _push_to_supabase(client: httpx.AsyncClient, rows: list[dict]):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("Supabase credentials not set â skipping cloud upload")
        return
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    # Send in batches of 50
    for i in range(0, len(rows), 50):
        batch = rows[i : i + 50]
        payload = [
            {
                "product_name": r["product_name"],
                "platform": r["platform"],
                "brand_name": r["brand_name"],
                "ad_text": r["ad_text"],
                "video_url": r["video_url"],
                "creative_url": r["creative_url"],
                "start_date": r["start_date"],
                "run_duration_days": r["run_duration_days"],
                "ad_id": r["ad_id"],
                "raw_metadata": r["raw_metadata"],
                "price": r.get("price"),
                "landing_page_url": r.get("landing_page_url"),
                "direct_video_url": r.get("direct_video_url"),
                "direct_ad_url": r.get("direct_ad_url"),
            }
            for r in batch
        ]
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code in (200, 201):
            logger.info("Supabase batch %d\u2013%d uploaded", i, i + len(batch))
        else:
            logger.error("Supabase error %s: %s", resp.status_code, resp.text)

# ---------------------------------------------------------------------------
# Apify helpers
# ---------------------------------------------------------------------------

APIFY_BASE = "https://api.apify.com/v2"

async def _start_actor(client: httpx.AsyncClient, actor_id: str, run_input: dict) -> str:
    """Start an Apify actor and return the run ID."""
    url = f"{APIFY_BASE}/acts/{actor_id}/runs"
    params = {"token": APIFY_TOKEN}
    resp = await client.post(url, params=params, json=run_input, timeout=60)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    logger.info("Started Apify actor %s \u2192 run %s", actor_id, run_id)
    return run_id


async def _poll_run(client: httpx.AsyncClient, run_id: str, timeout: int = 300) -> dict:
    """Poll an Apify run until it finishes (SUCCEEDED / FAILED / ABORTED / TIMED-OUT)."""
    url = f"{APIFY_BASE}/actor-runs/{run_id}"
    params = {"token": APIFY_TOKEN}
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = await client.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()["data"]
        status = data.get("status")
        logger.info("Run %s status: %s", run_id, status)
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            return data
        await asyncio.sleep(10)
    raise TimeoutError(f"Apify run {run_id} did not finish within {timeout}s")


async def _get_dataset(client: httpx.AsyncClient, dataset_id: str) -> list[dict]:
    """Fetch all items from an Apify dataset."""
    url = f"{APIFY_BASE}/datasets/{dataset_id}/items"
    params = {"token": APIFY_TOKEN, "format": "json"}
    resp = await client.get(url, params=params, timeout=120)
    resp.raise_for_status()
    items = resp.json()
    logger.info("Fetched %d items from dataset %s", len(items), dataset_id)
    return items

# ---------------------------------------------------------------------------
# Normalisation & filtering
# ---------------------------------------------------------------------------

def _parse_date(val) -> Optional[datetime]:
    """Try to parse a date from various formats."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val, tz=timezone.utc)
        except (OSError, ValueError):
            return None
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                     "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
                     "%Y-%m-%dT%H:%M:%S%z", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(val, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
    return None


def _extract_price(text: str) -> Optional[str]:
    """Extract the first price-like pattern from text."""
    if not text:
        return None
    match = re.search(r'[\$\â¬\Â£\Â¥][\d,]+\.?\d*|\d+[\.,]\d{2}\s*[\$\â¬\Â£\Â¥USD EUR GBP]', text)
    return match.group(0).strip() if match else None


def _normalise_meta(items: list[dict], product_name: str) -> list[dict]:
    now = datetime.now(timezone.utc)
    results = []
    for item in items:
        start = _parse_date(item.get("startDate") or item.get("ad_delivery_start_time"))
        end = _parse_date(item.get("endDate") or item.get("ad_delivery_stop_time"))
        if start is None:
            continue
        run_days = (now - start).days if end is None else (end - start).days
        if run_days < MIN_RUN_DAYS:
            continue
        last_active = end or now
        if (now - last_active).days > ACTIVE_DAYS_LIMIT:
            continue
        # Extract video URL
        video_url = ""
        snapshot = item.get("snapshot", {}) or {}
        videos = snapshot.get("videos") or []
        if videos and isinstance(videos, list):
            video_url = videos[0].get("video_sd_url") or videos[0].get("video_hd_url") or ""
        # Extract image / creative URL
        creative_url = ""
        images = snapshot.get("images") or []
        if images and isinstance(images, list):
            creative_url = images[0].get("original_image_url") or images[0].get("resized_image_url") or ""
        if not creative_url:
            cards = snapshot.get("cards") or []
            if cards and isinstance(cards, list):
                creative_url = cards[0].get("resized_image_url") or cards[0].get("original_image_url") or ""
        ad_text = (snapshot.get("body", {}) or {}).get("text", "") or item.get("ad_creative_bodies", [""])[0] if isinstance(item.get("ad_creative_bodies"), list) else ""
        if not ad_text:
            ad_text = item.get("body") or ""

        # --- NEW: Extract enriched fields ---
        ad_id_val = item.get("adArchiveID") or item.get("id") or ""
        price = _extract_price(ad_text)
        # Landing page URL
        landing_page_url = (
            snapshot.get("link_url")
            or item.get("website_url")
            or (snapshot.get("cta", {}) or {}).get("value", {}).get("link")
            or item.get("ad_creative_link_url")
            or ""
        ) or None
        # Direct video URL (HD preferred)
        direct_video_url = None
        if videos and isinstance(videos, list):
            direct_video_url = videos[0].get("video_hd_url") or videos[0].get("video_sd_url") or None
        # Direct ad URL
        direct_ad_url = f"https://www.facebook.com/ads/library/?id={ad_id_val}" if ad_id_val else None

        results.append({
            "product_name": product_name,
            "platform": "Meta",
            "brand_name": item.get("pageName") or item.get("page_name") or "",
            "ad_text": ad_text[:5000],
            "video_url": video_url,
            "creative_url": creative_url,
            "start_date": start.strftime("%Y-%m-%d"),
            "run_duration_days": run_days,
            "ad_id": str(ad_id_val),
            "raw_metadata": json.dumps(item, default=str)[:10000],
            "price": price,
            "landing_page_url": landing_page_url,
            "direct_video_url": direct_video_url,
            "direct_ad_url": direct_ad_url,
        })
    logger.info("Meta normalisation: %d/%d passed filters", len(results), len(items))
    return results


def _normalise_tiktok(items: list[dict], product_name: str) -> list[dict]:
    now = datetime.now(timezone.utc)
    results = []
    for item in items:
        create_time = item.get("createTime") or item.get("createTimeISO")
        start = _parse_date(create_time)
        if start is None:
            continue
        run_days = (now - start).days
        if run_days < MIN_RUN_DAYS:
            continue
        if run_days > (ACTIVE_DAYS_LIMIT + MIN_RUN_DAYS + 365):
            continue
        author = item.get("authorMeta") or {}
        video_url = item.get("videoUrl") or ""
        if not video_url:
            video_meta = item.get("videoMeta") or {}
            video_url = video_meta.get("downloadAddr") or ""
        covers = item.get("covers") or []
        creative_url = covers[0] if covers else (item.get("cover") or "")

        # --- NEW: Extract enriched fields ---
        ad_id_val = str(item.get("id", ""))
        ad_text_val = (item.get("text") or item.get("desc") or "")[:5000]
        price = _extract_price(ad_text_val)
        # Landing page URL
        landing_page_url = (
            item.get("webVideoUrl")
            or (item.get("video") or {}).get("webVideoUrl")
            or item.get("external_url")
            or None
        )
        # Direct video URL
        direct_video_url = (
            (item.get("video") or {}).get("downloadAddr")
            or item.get("videoUrl")
            or None
        )
        # Direct ad URL
        author_username = author.get("uniqueId") or author.get("name") or ""
        direct_ad_url = f"https://www.tiktok.com/@{author_username}/video/{ad_id_val}" if author_username and ad_id_val else None

        results.append({
            "product_name": product_name,
            "platform": "TikTok",
            "brand_name": author.get("name") or author.get("nickName") or item.get("author", ""),
            "ad_text": ad_text_val,
            "video_url": video_url,
            "creative_url": creative_url,
            "start_date": start.strftime("%Y-%m-%d"),
            "run_duration_days": run_days,
            "ad_id": ad_id_val,
            "raw_metadata": json.dumps(item, default=str)[:10000],
            "price": price,
            "landing_page_url": landing_page_url,
            "direct_video_url": direct_video_url,
            "direct_ad_url": direct_ad_url,
        })
    logger.info("TikTok normalisation: %d/%d passed filters", len(results), len(items))
    return results

# ---------------------------------------------------------------------------
# Core scrape pipeline
# ---------------------------------------------------------------------------

async def run_scrape(product_name: str):
    """Full scrape pipeline for one product."""
    logger.info("=== Starting scrape for: %s ===", product_name)
    async with httpx.AsyncClient() as client:
        # 1 \ Start both actors in parallel
        meta_input = {
            "searchQuery": product_name,
            "countryCode": COUNTRY_CODE,
            "maxItems": 200,
        }
        tiktok_input = {
            "searchQueries": [product_name],
            "resultsPerPage": 100,
        }
        meta_run_id, tiktok_run_id = await asyncio.gather(
            _start_actor(client, "apify/facebook-ads-scraper", meta_input),
            _start_actor(client, "clockworks/tiktok-scraper", tiktok_input),
        )

        # 2 \ Poll both runs in parallel
        meta_run, tiktok_run = await asyncio.gather(
            _poll_run(client, meta_run_id),
            _poll_run(client, tiktok_run_id),
        )

        # 3 \ Fetch datasets
        all_ads: list[dict] = []

        if meta_run["status"] == "SUCCEEDED":
            meta_dataset_id = meta_run["defaultDatasetId"]
            meta_items = await _get_dataset(client, meta_dataset_id)
            all_ads.extend(_normalise_meta(meta_items, product_name))
        else:
            logger.error("Meta run failed: %s", meta_run["status"])

        if tiktok_run["status"] == "SUCCEEDED":
            tiktok_dataset_id = tiktok_run["defaultDatasetId"]
            tiktok_items = await _get_dataset(client, tiktok_dataset_id)
            all_ads.extend(_normalise_tiktok(tiktok_items, product_name))
        else:
            logger.error("TikTok run failed: %s", tiktok_run["status"])

        logger.info("Total winning ads found: %d", len(all_ads))

        if not all_ads:
            logger.info("No winning ads to save")
            return {"total": 0, "meta": 0, "tiktok": 0}

        # 4 \ Save to local SQLite
        conn = _get_db()
        try:
            _insert_local(conn, all_ads)
        finally:
            conn.close()

        # 5 \ Save to Supabase
        await _push_to_supabase(client, all_ads)

        meta_count = sum(1 for a in all_ads if a["platform"] == "Meta")
        tiktok_count = sum(1 for a in all_ads if a["platform"] == "TikTok")
        logger.info("=== Scrape complete: %d Meta, %d TikTok ===", meta_count, tiktok_count)
        return {"total": len(all_ads), "meta": meta_count, "tiktok": tiktok_count}

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure DB exists on startup
    conn = _get_db()
    conn.close()
    logger.info("Winning Ad Scraper started")
    yield
    logger.info("Winning Ad Scraper shutting down")

app = FastAPI(
    title="Winning Ad Scraper",
    description="Webhook-triggered scraper for Meta & TikTok winning ads",
    version="1.0.0",
    lifespan=lifespan,
)


class ScrapeRequest(BaseModel):
    product_name: str


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/webhook")
async def webhook(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """Trigger a scrape in the background \u2014 returns immediately."""
    logger.info("Webhook received for product: %s", req.product_name)
    background_tasks.add_task(run_scrape, req.product_name)
    return {
        "status": "accepted",
        "product_name": req.product_name,
        "message": "Scrape started in background",
    }


@app.post("/scrape/sync")
async def scrape_sync(req: ScrapeRequest):
    """Run a scrape synchronously (for testing). Returns results when done."""
    logger.info("Sync scrape requested for product: %s", req.product_name)
    try:
        result = await run_scrape(req.product_name)
        return {"status": "completed", **result}
    except Exception as exc:
        logger.exception("Scrape failed")
        raise HTTPException(status_code=500, detail=str(exc))
