# Hourly_Table_Final

Hourly aggregation pipeline that reads 5-minute granularity data from `metrics.ai_metrics_5m_v2` and aggregates it into `metrics.ai_service_features_hourly` in ClickHouse.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running

```bash
python3 hourly_aggregation_pipeline.py
```

Runs immediately on start, then automatically re-runs every hour at `:25` (uses system local time). Keep the process running — no external cron needed.

## How it works

- Reads from `ai_metrics_5m_v2` (5-min windows) and writes to `ai_service_features_hourly`
- Each hour produces 2 rows: one for `metric='success_rate'` and one for `metric='latency'`
- Only the latest in-flight hour is protected — all older hours are aggregated even if incomplete
- For gaps < 24 hours: processes one hour at a time
- For gaps >= 24 hours (e.g. after downtime): processes in 24-hour batches for speed
- Safe to re-run — `ReplacingMergeTree(updated_at)` prevents duplicates

## State / Audit

Every run writes one row to `metrics.hourly_pipeline_state` with status (`success`, `up_to_date`, `noop`), hours processed, and duration. This table is audit-only — the pipeline never reads from it.

## Configuration

Credentials and host are set at the top of `hourly_aggregation_pipeline.py` (lines 29-34). Update before deploying to a new environment.
