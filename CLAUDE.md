# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A ClickHouse hourly aggregation pipeline that reads 5-minute granularity data from `ai_metrics_5m` and aggregates it into `ai_service_features_hourly`. There are **two files with this same logic** — see "Two Pipeline Files" below before editing either one.

## Two Pipeline Files (read before editing)

- **`hourly_aggregation_pipeline.py`** — the original standalone script. Runs its own `schedule` loop directly.
- **`hourly_pipeline.py`** — near-identical copy, restructured to be importable: a lazy `get_pipeline()` singleton and a `run_hourly_job()` entrypoint, so it can be imported by a shared `scheduler.py` process (not present in this repo — only discussed alongside a `producer_opensearch.py` script that also isn't in this repo). Still runnable standalone too.

Both files currently point at the **same production tables** via the same `.env` (see Configuration). This is a known, unresolved duplication — every fix to `ch_datetime()`, the aggregation SQL, the batch logic, or the two-metric invariant must currently be applied to **both files** or they will silently drift apart. If you're asked to change core pipeline logic and only one of these files is mentioned, check whether the other needs the same change too. Nothing currently prevents both from being scheduled and run at once (harmless due to `ReplacingMergeTree` dedup, but wasteful and confusing).

## Running the Pipeline

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 hourly_aggregation_pipeline.py   # or: python3 hourly_pipeline.py
```

The script runs immediately on start, then schedules itself to re-run every hour at `:30` using the `schedule` library — no external cron needed. It runs until the process is killed. Scheduling uses **local system time**.

**Legacy cron (if running outside a container):**
```bash
30 * * * * cd /path/to/script && python3 hourly_aggregation_pipeline.py >> /var/log/hourly_aggregation/cron.log 2>&1
```

## Key Architecture Principles

**Monotonic Processing with Partial Hours**
- Only the latest in-flight hour is protected (`max(ts) - 1 HOUR`)
- All older hours are aggregated even if incomplete — partial hours are by design (~81% of records have < 12 windows)
- No lookbacks or gap scanning — processes forward from the last completed hour
- Source of truth is always `ai_metrics_5m`

**Batch Processing Strategy**
- Gap < 24 hours: process one-by-one
- Gap >= 24 hours: process in 24-hour batches, then remaining hours one-by-one
- Optimized for long downtime recovery (e.g. 1,980 hours in ~88 seconds)

**Two-Metric Invariant**
- An hour is only considered "complete" when BOTH `success_rate` AND `latency` metrics exist (`COUNT(DISTINCT metric) = 2`)
- This prevents skipping ahead if one metric fails and enables automatic recovery

**State Table is Audit-Only**
- `metrics.hourly_pipeline_state` (ClickHouse) stores one row per pipeline run — never read by the pipeline for processing decisions
- Pipeline determines processing range by querying the database directly (`ai_service_features_hourly` + `ai_metrics_5m`)
- Each row records: `run_id`, `started_at`, `finished_at`, `status`, `source_latest_safe_hour`, `last_processed_hour_before_run`, `first_hour_processed`, `last_hour_processed`, `total_hours_processed`, `batch_mode`, `batch_count`, `duration_seconds`. Status values: `noop` (no source data at all), `up_to_date` (already caught up), `success` (hours processed).

**DateTime Handling**
- `ch_datetime()` handles ClickHouse DateTime values that may be returned as integers (Unix timestamps) or ISO strings over HTTP JSON — always use this when parsing timestamp fields from query results
- ClickHouse HTTP JSON returns `0` (integer) for NULL DateTime columns. `ch_datetime()` catches this via `value == 0` for ints and `year < 2000` guard for ISO strings (epoch 0 = `1970-01-01`). Both return `None`.

**First-Run Bootstrap**
- `get_earliest_hour_from_5min()` returns the oldest hour in the source table and is used as `start_hour` when there is no prior hourly data. On subsequent runs, `start_hour` is `last_hourly + 1 hour`.

**Table Auto-Creation**
- `ensure_hourly_table()` and `ensure_state_table()` are called at the top of every `run()` invocation (`CREATE TABLE IF NOT EXISTS`). They are safe to re-run and add no overhead once tables exist.

**Fault Tolerance**
- The `job()` wrapper at the entry point catches all exceptions and logs them without re-raising, so the `schedule` loop keeps running even after a failed run. A 30-second sleep polls the scheduler between runs.

**Source Deduplication (ReplacingMergeTree)**
- `ai_metrics_5m` is `ReplacingMergeTree(ingestion_time)` — a late correction to a 5-min window inserts a second row under the same `(application_id, project_id, service_id, ts)` key rather than updating in place. ClickHouse only collapses these duplicates on background merge, or immediately with `FINAL`.
- `aggregate_success_rate()` and `aggregate_latency()` therefore read `FROM ai_metrics_5m FINAL`. Without `FINAL`, any not-yet-merged duplicate rows get double-counted into `SUM`/`AVG`/`COUNT`, inflating `total_requests` and `response_breach_count` — measured at ~0.9–1.3% before this was added. `FINAL` forces an on-the-fly merge at query time, so it's slower than a plain scan; watch this on very large batch backfills.

## Critical Code Sections

**Never modify:**
- `ch_datetime()` — handles both int and string timestamp formats from ClickHouse HTTP API
- `get_latest_safe_hour_from_5min()` — protects the in-flight hour
- `get_latest_hourly_hour()` — enforces the two-metric invariant with `COUNT(DISTINCT metric) = 2`
- GROUP BY clauses in aggregation queries — ensures per-hour separation even in batch mode
- `FINAL` on the `ai_metrics_5m` reads in `aggregate_success_rate()`/`aggregate_latency()` — removing it reintroduces duplicate-counting from un-merged `ReplacingMergeTree` rows (see Source Deduplication above)

**Safe to modify:**
- Batch size: `timedelta(hours=24)` in the `hours_remaining >= 24` branch of `run()`
- Batch threshold: `if total_hours >= 24` in `run()`
- `.env` values (see Configuration below) — never hardcode credentials or table names back into the `.py` file

## Configuration

All ClickHouse connection details and table names are loaded from a `.env` file in the script's directory via `python-dotenv` (`load_dotenv(..., override=True)`), read into module-level constants at import time: `CH_HOST`, `CH_PORT`, `CH_USERNAME`, `CH_PASSWORD`, `CH_DATABASE`, `CH_DATA_TABLE` (5-min source), `CH_HOURLY_TABLE` (rollup target), `CH_STATE_TABLE` (audit table). All are required env vars (`os.environ[...]`, no defaults) — missing any of them crashes on import with a `KeyError`.

The env var names intentionally match a separate `producer_opensearch.py` script's `.env` convention (`CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_DATA_TABLE`) — that script is **not part of this repo**, it was designed to run alongside this pipeline in the same pod and feed `ai_metrics_5m` (referenced here as background context only). `CLICKHOUSE_DATA_TABLE` refers to that same physical table (`ai_metrics_5m`). `CLICKHOUSE_HOURLY_TABLE` and `CLICKHOUSE_STATE_TABLE` are unique to this pipeline.

`.env` is gitignored — never commit it. `override=True` means `.env` values win over any pre-existing OS/pod env vars of the same name; if this script is ever containerized, make sure `.env` isn't baked into the image (add it to `.dockerignore` too) and prefer injecting real values via a K8s Secret rather than shipping a `.env` file.

## Database Schema

**Source:** `metrics.ai_metrics_5m` — `ReplacingMergeTree(ingestion_time)`, 5-min windows. Fields used by the pipeline: `application_id`, `service_id`, `project_id`, `service`, `ts` (grouping/filtering), `success_rate`, `success_target`, `response_success_rate`, `response_target_percent`, `total_count`, `response_breach_count`, `sum_response_time`, `p90_latency`. Additional fields present but not yet used: `application_name`, `success_count`, `error_count`, `error_rate`, `response_slo_seconds`, `avg_latency`, `p80_latency`, `p95_latency`, `burn_rate`, `eb_health`, `response_health`, `region`, `deploy_version`, `ingestion_time`, `day_of_week`, `week_of_month`, `hour`, `minute_bucket`

**Target:** `metrics.ai_service_features_hourly` — `ReplacingMergeTree(updated_at)`, ordered by `(application_id, service_id, project_id, service, metric, ts_hour)`, partitioned by `toYYYYMM(ts_hour)`. Includes `project_id UInt64` matching the source type in `ai_metrics_5m`.

Each hour produces two independent rows: one with `metric='success_rate'` and one with `metric='latency'`. Both rows read ALL 5-minute windows but use different source fields. The latency row has non-NULL values for `response_breach_count`, `avg_latency`, and `p90_latency`; the success_rate row has NULL for those. `p90_latency` is a weighted approximation: `SUM(p90_latency * total_count) / SUM(total_count)`.

## Idempotency

`ReplacingMergeTree(updated_at)` ensures safe reprocessing — multiple concurrent runs will not create duplicates. Pipeline always starts from `last_completed_hour + 1`.

## ClickHouseClient Implementation Details

- Uses raw **HTTPS** (`requests.post`) on port 443 — **no ClickHouse driver installed**. `requirements.txt` only has `requests` and `schedule`.
- SSL certificate verification is disabled (`verify=False`) with `urllib3` warnings suppressed — kept for compatibility; update to `verify=True` if the host cert is trusted by system CA roots.
- `execute(query)` — fire-and-forget; returns raw text.
- `execute_json(query)` — automatically appends `FORMAT JSONEachRow` before sending, then parses each response line as JSON. Never add `FORMAT` yourself when calling `execute_json`.
- HTTP timeout is **300 seconds** per request. For very large batch sizes this may need increasing.
- `_save_state` uses Python f-string interpolation (not parameterized queries) — acceptable here since all values are internal pipeline state, never user input.
- **State dict key naming quirk**: the Python dict uses `"last_processed_before_run"` but the DB column is `last_processed_hour_before_run`. Both sides of `_save_state` are consistent with their own naming — don't "fix" the mismatch without updating both.
- **Credentials**: `CH_HOST`, `CH_USERNAME`, `CH_PASSWORD` (loaded from `.env`, see Configuration above) are real production values. Do not log, print, or copy them into other files, and don't hardcode them back into the `.py` file.

## Verifying Runs

Check the last few pipeline runs:
```sql
SELECT run_id, started_at, status, total_hours_processed, batch_mode, duration_seconds
FROM metrics.hourly_pipeline_state
ORDER BY started_at DESC
LIMIT 10
FORMAT Pretty;
```

Check hourly output coverage:
```sql
SELECT min(ts_hour), max(ts_hour), count(DISTINCT ts_hour) AS distinct_hours
FROM metrics.ai_service_features_hourly
FORMAT Pretty;
```

## No Test Suite

There are no tests, no linting config, and no CI in this repo. The pipeline is verified by observing `hourly_pipeline_state` audit rows and querying `ai_service_features_hourly` directly.
