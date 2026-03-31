# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A single-file ClickHouse hourly aggregation pipeline (`hourly_aggregation_pipeline.py`) that reads 5-minute granularity data from `ai_metrics_5m_v2` and aggregates it into `ai_service_features_hourly`.

## Running the Pipeline

```bash
pip3 install -r requirements.txt
python3 hourly_aggregation_pipeline.py
```

The script runs immediately on start, then schedules itself to re-run every hour at `:25` using the `schedule` library — no external cron needed. It runs until the process is killed. Scheduling uses **local system time**.

**Legacy cron (if running outside a container):**
```bash
25 * * * * cd /path/to/script && python3 hourly_aggregation_pipeline.py >> /var/log/hourly_aggregation/cron.log 2>&1
```

## Key Architecture Principles

**Monotonic Processing with Partial Hours**
- Only the latest in-flight hour is protected (`max(ts) - 1 HOUR`)
- All older hours are aggregated even if incomplete — partial hours are by design (~81% of records have < 12 windows)
- No lookbacks or gap scanning — processes forward from the last completed hour
- Source of truth is always `ai_metrics_5m_v2`

**Batch Processing Strategy**
- Gap < 24 hours: process one-by-one
- Gap >= 24 hours: process in 24-hour batches, then remaining hours one-by-one
- Optimized for long downtime recovery (e.g. 1,980 hours in ~88 seconds)

**Two-Metric Invariant**
- An hour is only considered "complete" when BOTH `success_rate` AND `latency` metrics exist (`COUNT(DISTINCT metric) = 2`)
- This prevents skipping ahead if one metric fails and enables automatic recovery

**State Table is Audit-Only**
- `metrics.hourly_pipeline_state` (ClickHouse) stores one row per pipeline run — never read by the pipeline for processing decisions
- Pipeline determines processing range by querying the database directly (`ai_service_features_hourly` + `ai_metrics_5m_v2`)
- Each row records: `run_id`, `started_at`, `finished_at`, `status`, `source_latest_safe_hour`, `last_processed_hour_before_run`, `first_hour_processed`, `last_hour_processed`, `total_hours_processed`, `batch_mode`, `batch_count`, `duration_seconds`. Status values: `noop` (no source data at all), `up_to_date` (already caught up), `success` (hours processed).

**DateTime Handling**
- `ch_datetime()` (lines 72-88) handles ClickHouse DateTime values that may be returned as integers (Unix timestamps) or ISO strings over HTTP JSON — always use this when parsing timestamp fields from query results

## Critical Code Sections

**Never modify:**
- Lines 72-88: `ch_datetime()` — handles both int and string timestamp formats from ClickHouse HTTP API
- Lines 156-162: `get_latest_safe_hour_from_5min()` — protects the in-flight hour
- Lines 164-175: `get_latest_hourly_hour()` — enforces the two-metric invariant with `COUNT(DISTINCT metric) = 2`
- GROUP BY clauses in aggregation queries — ensures per-hour separation even in batch mode

**Safe to modify:**
- Batch size (currently 24 hours, line 331 — `timedelta(hours=24)`)
- Batch threshold (currently 24 hours, line 323 — `if total_hours >= 24`)
- `CH_STATE_TABLE` name (line 34)
- Database credentials (lines 29-33) — consider moving to environment variables

## Database Schema

**Source:** `metrics.ai_metrics_5m_v2` — 5-min windows. Fields used by the pipeline: `success_rate`, `success_target`, `response_success_rate`, `response_target_percent`, `total_count`, `response_breach_count`, `sum_response_time`, `p90_latency`. Additional fields present but not yet used: `application_name`, `project_id`, `success_count`, `error_count`, `error_rate`, `response_slo_seconds`, `avg_latency`, `p80_latency`, `p95_latency`, `burn_rate`, `eb_health`, `response_health`, `region`, `deploy_version`, `ingestion_time`, `processed_window`

**Target:** `metrics.ai_service_features_hourly` — `ReplacingMergeTree(updated_at)`, ordered by `(application_id, service_id, service, metric, ts_hour)`, partitioned by `toYYYYMM(ts_hour)`. Includes `project_id Int64` sourced from `ai_metrics_5m_v2`.

Each hour produces two independent rows: one with `metric='success_rate'` and one with `metric='latency'`. Both rows read ALL 5-minute windows but use different source fields. The latency row has non-NULL values for `response_breach_count`, `avg_latency`, and `p90_latency`; the success_rate row has NULL for those. `p90_latency` is a weighted approximation: `SUM(p90_latency * total_count) / SUM(total_count)`.

## Idempotency

`ReplacingMergeTree(updated_at)` ensures safe reprocessing — multiple concurrent runs will not create duplicates. Pipeline always starts from `last_completed_hour + 1`.
