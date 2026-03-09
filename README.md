# Winning Ad Scraper

A self-hosted webhook service that scrapes **Meta Ad Library** and **TikTok** for winning ads (running 7+ days, active in last 30 days) for a given product name.

## Stack
- **FastAPI** Ã¢ÂÂ webhook server
- **Apify** Ã¢ÂÂ Meta & TikTok scraping
- **Supabase** Ã¢ÂÂ cloud storage
- **SQLite** Ã¢ÂÂ local storage

## Quick Start

### 1. Clone & configure
```bash
git clone https://github.com/hamzasmartlinker/winning-ad-scraper
cd winning-ad-scraper
cp .env.example .env
# Fill in your .env values
```

### 2. Run with Docker
```bash
docker-compose up -d
```

### 3. Run locally
```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

## Webhook Usage

```
POST http://localhost:8000/webhook
Content-Type: application/json

{"product_name": "Type C charger"}
```

Returns immediately. Scraping runs in background and results are saved to SQLite + Supabase.

## Flutter Integration

```dart
await http.post(
  Uri.parse('http://YOUR_SERVER:8000/webhook'),
  headers: {'Content-Type': 'application/json'},
  body: jsonEncode({'product_name': productNameFromGemini}),
);
```

## Supabase Table Schema

```sql
CREATE TABLE winning_ads (
  id                BIGSERIAL PRIMARY KEY,
  product_name      TEXT NOT NULL,
  platform          TEXT NOT NULL,  -- 'Meta' or 'TikTok'
  brand_name        TEXT,
  ad_text           TEXT,
  video_url         TEXT,
  creative_url      TEXT,
  start_date        TEXT,
  run_duration_days INTEGER,
  ad_id             TEXT,
  raw_metadata      TEXT,
  price             TEXT,            -- price from ad text (e.g. "$19.99")
  landing_page_url  TEXT,            -- advertiser's destination URL
  direct_video_url  TEXT,            -- direct playable video link
  direct_ad_url     TEXT,            -- link to the ad/post itself
  created_at        TIMESTAMPTZ DEFAULT NOW()
);
```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| APIFY_TOKEN | Apify API token | required |
| SUPABASE_URL | Supabase project URL | required |
| SUPABASE_SERVICE_KEY | Supabase service role key | required |
| SUPABASE_TABLE | Target table name | winning_ads |
| COUNTRY_CODE | ISO country code | DZ |
| MIN_RUN_DAYS | Min days ad must have run | 7 |
| ACTIVE_DAYS_LIMIT | Active within N days | 30 |
