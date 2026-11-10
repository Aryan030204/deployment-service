# deployment-service

Tenant onboarding orchestrator for a multi-tenant Shopify analytics platform. Given a brand's
Shopify app credentials, it provisions everything that brand needs — OAuth access token, a
dedicated MySQL database, an API key, tenant/pipeline config records — and then backfills ~60 days
of analytics data into the new database. It also handles tenant teardown.

One HTTP call in, a fully onboarded and backfilled tenant out.

## Entry points

| Entry point | Command | Role |
|---|---|---|
| **API** | `python services/deployment-service/main.py` | FastAPI app (`services/api_service.py`). Accepts webhooks, encrypts secrets, pushes jobs to Redis. Does no pipeline work. |
| **Worker** | `python services/deployment-service/main_worker.py` | Pops jobs from Redis and runs the 9-step pipeline (`services/orchestrator.py`). This is where all the work happens. |
| **CLI** | `python delete_tenant.py <brand_id>` | Standalone tenant deletion, same logic as the DELETE endpoint. |

Both processes run from the same image; `docker-compose.yml` starts one of each.

### HTTP surface

Both routes require the header `x-pipeline-key: $PIPELINE_AUTH_HEADER`.

- `POST /tenant/onboard` → `202` `{status, job_id, brand_id}`. Enqueues; returns immediately.
  `409` if that `brand_id` already has an in-flight job.
  Body: `brand_id`, `brand_name`, `client_id`, `client_secret`, `shop_name` (required);
  `code`, `brand_url`, `website_url`, `app_id_mapping` (optional).
- `DELETE /tenant/{brand_id}` → `200`. Runs **synchronously in the API process** (not queued).

## Data flow map

```
 Caller (platform admin / provisioning UI)
   │  POST /tenant/onboard  { brand_id, brand_name, client_id, client_secret, shop_name, ... }
   │  x-pipeline-key
   ▼
┌───────────────────────────────────────────────┐
│ API process — main.py → api_service.py        │
│  • verify x-pipeline-key                      │
│  • AES-256-CBC encrypt client_id / secret     │  shared/utils/encryption.py
│  • enqueue                                    │
└───────────────────────────────────────────────┘
   │
   ▼  Redis (services/onboarding_queue.py)
   ├─ LIST  deployment-service:onboarding:queue          ← job payloads (RPUSH / BLPOP)
   ├─ HASH  deployment-service:onboarding:job:<job_id>   ← status, timestamps, error, payload
   └─ STR   deployment-service:onboarding:active:<brand> ← SETNX dedupe lock, cleared on finish
   │
   ▼
┌───────────────────────────────────────────────┐
│ Worker process — main_worker.py               │
│  worker_service.py: N threads (PARALLEL_WORKERS), each BLPOP → run_pipeline  │
└───────────────────────────────────────────────┘
   │
   ▼  orchestrator.run_pipeline(job)  — sequential, fail-fast, NUM_OF_RETRIES per step
   │
   │   payload is built once by workflow.ensure_workflow_payloads() into
   │   `tenant_payload` + `pipelinecreds_payload`, then threaded through every step
   │
   ├─ 1  generate_access_token.py     Shopify OAuth  → access_token
   │     fetch_shop_currency.py       GET /shop.json → currency + iana_timezone
   ├─ 2  clone_database.py            MySQL: CREATE DATABASE <BRAND> + CREATE TABLE … LIKE
   │                                  (schema copied from newest row in master.brands)
   │                                  then INSERT into master.brands
   ├─ 3  generate_speed_token.py      POST GET_SPEED_TOKEN_API → speed_key (stored encrypted)
   │                                  + upsert website_url into Mongo pagespeed_brands.brands
   ├─ 4  create_tenant_record.py      POST CREATE_TENANT_API  (→ Mongo arch-auth.tenants)
   │                                  app_id_mapping derived from Shopify GraphQL if absent
   ├─ 5  create_pipelinecreds.py      POST CREATE_PIPELINECREDS_API (→ arch-auth.pipelinecreds)
   │                                  ── steps 4/5 are what make the tenant discoverable ──
   ├─ 6  backfill_data.py             main ETL: Shopify Admin REST orders + ShopifyQL sessions
   ├─ 7  backfill_products_pipeline_data.py   product sessions + materialized views
   ├─ 8  backfill_hourly_products_data.py     hourly product sessions/performance
   └─ 9  backfill_utm_aggregation.py          UTM daily + hourly aggregate tables
   │
   ▼
 Redis job hash → status=succeeded|failed;  active lock released
 Every step boundary is also POSTed as a log line to each URL in LOGS_URL
```

### Where each backfill step gets its config

Step 6 reads credentials **straight from the in-flight payload** (`pipelinecreds_payload` /
`tenant_payload`) — no external lookup. Steps 7–9 instead re-read the tenant from
`GET {GET_BRANDS_API}/{brand_id}` and decrypt `db_password` / `access_token` with
`PASSWORD_AES_KEY`. That means **steps 4 and 5 must have succeeded** for 7–9 to work; they
consume the records those steps wrote.

### Backfill windows (IST, `scripts/backfill_common.py`)

- Steps 6, 7, 9: yesterday − 60 days → yesterday (`ONBOARDING_BACKFILL_LOOKBACK_DAYS`).
- Step 8: `PRODUCTS_PIPELINE_DATE_RANGE` days ending yesterday (default 7).

### Tables written into the tenant database

`shopify_orders`, `shopify_orders_update`, `returns_fact`, `sessions_summary`,
`hourly_sessions_summary`, `hourly_sessions_summary_shopify`, `shopify_orders_utm_daily`,
`overall_utm_summary`, `overall_referrer_summary`, `pipeline_metadata`, the
`*_summary` / `*_summary_stage` family (sales, order, discount, gross, hour_wise, overall),
`product_sessions_snapshot`, `product_landing_mapping`, `hourly_product_sessions`,
`hourly_product_performance_rollup`, `mv_product_sessions_by_*_daily`, and the
`utm_*_daily` / `utm_*_hourly` aggregate tables.

Summaries are rebuilt **incrementally** — only for dates touched by the orders just ingested —
via staging tables merged into the main tables.

### Deletion flow

`DELETE /tenant/{brand_id}` → `scripts/delete_tenant_logic.py`: look up the brand in
`master.brands` → `DROP DATABASE` → delete the `master.brands` row → delete from Mongo
(`arch-auth.tenants`, `arch-auth.pipelinecreds` on both clusters, `pagespeed_brands.brands`).
MySQL is authoritative; Mongo failures are logged but do not fail the request.

## Naming conventions

Derived from `brand_name` and enforced throughout — worth knowing, since the backfills join on them:

- `brand_name.upper()` → database name, `tenant.brand_id`, `pipelinecreds.brand_name`
- `brand_name.lower()` → `brand_tag`, `shard_id`
- `brand_id` (int) → `brand_num`, and the `brand_index` used to key runtime state in `backfill_data.py`

## External dependencies

Redis (queue) · MySQL (`master` + one DB per tenant) · MongoDB (`arch-auth`, `pagespeed_brands`)
· Shopify Admin REST, GraphQL and ShopifyQL · internal tenant/pipelinecreds/speed-token APIs
· QStash (optional cache-invalidation webhook) · `LOGS_URL` sinks (optional).

## Configuration

All via environment (`.env` at repo root, loaded by `python-dotenv`).

**Required:** `PIPELINE_AUTH_HEADER` (shared secret for both inbound and outbound calls),
`PASSWORD_AES_KEY` (AES key, SHA-256 derived), `REDIS_URI`, `DB_HOST`, `DB_PASSWORD`, `DB_PORT`,
`DB_USER`, `MASTER_DB`, `READ_REPLICA`, `API_VERSION`, `GET_BRANDS_API`, `CREATE_TENANT_API`,
`CREATE_PIPELINECREDS_API`, `GET_SPEED_TOKEN_API`, `ARCH_DB_URI`, `SPEED_DB_URI`.

**Tuning:** `PORT` (default 8000), `PARALLEL_WORKERS` (default 1), `NUM_OF_RETRIES` (default 0),
`PRODUCTS_PIPELINE_DATE_RANGE` (7), `BACKFILL_CONCURRENCY` (5), `BATCH_SIZE` (1000),
`SHOPIFYQL_TIMEZONE` (Asia/Kolkata), `LOGS_URL` (single URL or JSON array).

**Cloning creds:** `MYSQL_ROOT_HOST` / `MYSQL_ROOT_USER` / `MYSQL_ROOT_PASSWORD` — step 2 needs
CREATE DATABASE rights and falls back to `DB_*` if unset.

**TLS:** `DB_TLS_CA_MODE` (`certifi` | `rds` | `custom` | `none`), `RDS_CA_PATH` /
`RDS_CA_BUNDLE_B64`, `CUSTOM_CA_PATH` / `CUSTOM_CA_BUNDLE_B64`, `DB_SSL_VERIFY_CERT`,
`DB_SSL_VERIFY_IDENTITY`. Connections retry without SSL if the TLS handshake fails.

**Optional:** `QSTASH_TOKEN`, `QSTASH_COMPLETION_TOKEN`, `ALERTS_WEBHOOK_URL`,
`TEST_MODE=true` (disables SSL and QStash, reads per-brand config from `*_<index>` env vars
instead of the tenant API).

## Running

```bash
docker network create shared-net    # once; compose expects it to exist
docker compose up --build           # API on :8002 → container $PORT, plus one worker

# or locally, from the repo root, with PYTHONPATH covering both roots:
PYTHONPATH=.:services/deployment-service python services/deployment-service/main.py
PYTHONPATH=.:services/deployment-service python services/deployment-service/main_worker.py

# smoke test
curl -X POST localhost:8002/tenant/onboard -H 'content-type: application/json' \
  -H "x-pipeline-key: $PIPELINE_AUTH_HEADER" -d @payload.json
```

## Layout

```
services/deployment-service/
  main.py / main_worker.py      process entry points
  services/
    api_service.py             FastAPI routes, auth, encryption, enqueue
    onboarding_queue.py        Redis queue + job status + per-brand dedupe
    worker_service.py          threaded BLPOP loop
    orchestrator.py            the 9 steps, retries, external logging
    workflow.py                payload construction + state constants
  scripts/                     one module per step, each exposing process_event(data)
    backfill_common.py         shared IST date-window helpers
shared/utils/                  encryption.py (AES-256-CBC), logger.py
assets/                        reference implementations the backfills were ported from
                               (Node product pipelines, the standalone Python ETL, the
                               UTM aggregation service, cloneDBImage.sql) — not executed
delete_tenant.py               CLI wrapper around the deletion logic
```

## Notes / gotchas

- **Not resumable.** A failed step aborts the job; earlier steps' side effects (created database,
  tenant record) remain. Re-running is an idempotent-ish retry — most writes are
  `CREATE … IF NOT EXISTS` / upserts — but nothing rolls back. The `STATE_*` constants in
  `workflow.py` are only set on the in-memory dict; they are not persisted anywhere.
- **Queue durability.** `BLPOP` removes the payload before the pipeline runs, so a worker crash
  mid-pipeline loses the job (the Redis hash keeps its `running` status and the active lock is
  never released, blocking re-onboarding of that brand until the key is cleared).
- **Step 2 picks its schema source dynamically** — the most recently inserted row in
  `master.brands`. A malformed newest tenant propagates into every subsequent onboarding.
- **`shared/` is imported as a top-level package**, so `PYTHONPATH` must include the repo root
  *and* `services/deployment-service`. The Dockerfile sets `PYTHONPATH=/app`; the scripts also
  patch `sys.path` themselves.
- `docker-compose.yml` maps host `8002` → container `8001`, but `api_service.py` defaults to
  `8000` — set `PORT=8001` in `.env` or the published port won't reach the app.
- Secrets are stored encrypted (`client_id`, `client_secret`, `speed_key`, `db_password`,
  `access_token`) and decrypted at point of use. `decrypt` accepts both the `iv:ciphertext` and
  raw base64 formats, and falls back to a NULL-padded key for values written by the Node APIs.
