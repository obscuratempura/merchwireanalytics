# Merchwire Brief

Merchwire Brief is a production-ready pipeline that ingests public Shopify product data and Meta Ad Library activity, computes pricing and advertising signals, and delivers daily and weekly email digests.

## Features

- Shopify ingestion with robots.txt compliance, per-host rate limiting, and ETag caching.
- Meta Ad Library aggregation with surge detection and attribution-ready summaries.
- Daily leaderboards, SKU movers, CSV exports, and Matplotlib charts stored in S3-compatible storage.
- FastAPI subscription API with verification links, Stripe webhook handling, and ESP unsubscribe support.
- Celery-powered scheduler targeting 07:30 PT weekdays for paid tiers and weekly Monday recap for free subscribers.
- Docker Compose stack with Postgres 15, Redis 7, API server, and Celery worker.
- Comprehensive tests: unit, integration with recorded fixtures, and end-to-end job execution.

## Setup

1. Install dependencies:

   ```bash
   pip install -e .
   ```

2. Copy the sample environment and update secrets:

   ```bash
   cp .env.example .env
   ```

3. Start the stack:

   ```bash
   make up
   ```

4. Apply the schema:

   ```bash
   make migrate
   ```

5. Seed demo brands and users:

   ```bash
   make seed
   ```

## Commands

- `make daily` – run the full daily pipeline (ingest, compute, export, email).
- `make weekly` – send the weekly free digest.
- `make send-test` – send a test email to `TEST_RECIPIENT` from `.env`.
- `make lint` / `make test` – run linting and unit/integration/e2e tests.

## Architecture Overview

```
app/
  ingest/        # Shopify + Meta ingestion clients
  logic/         # signal computation, ranking, charts, CSV export
  email/         # MJML template + renderer
  jobs/          # Celery entrypoints and orchestration scripts
  api/           # FastAPI app for subscriptions and archive access
  infra/         # Docker Compose + Procfile
```

Artifacts (charts, CSV) are stored under `artifacts/` and optionally uploaded to S3-compatible storage when credentials are provided.

## Testing

Run the full suite with:

```bash
make test
```

Integration fixtures live in `tests/fixtures` and simulate Shopify and Meta responses for two demo brands. The end-to-end test seeds the database, runs the daily job, and asserts email delivery, CSV creation, and chart outputs.

## Escalation

- **Shopify blocking** – the ingestor logs and skips the brand; no retries beyond policy.
- **Meta API errors** – warnings are logged and surfaced in Ops Slack; digest generation continues with available data.
- **Email delivery** – failures raise alerts via the ESP provider; resend is handled manually via `make daily` rerun.
- **Data anomalies** – ranking thresholds configurable via environment variables `MOVER_THRESHOLD`, `DISCOUNT_SPIKE_THRESHOLD`, etc.

