"""
Monotonic Hourly Aggregation Pipeline (Partial Hours Allowed)
------------------------------------------------------------
Rules:
- Source of truth: ai_metrics_5m_v2
- Only the latest in-flight hour is protected
- All older hours are aggregated even if incomplete
- No lookbacks, no gap scans
- ClickHouse state table (hourly_pipeline_state) = audit only

Batch Processing:
- Gap < 24 hours: Process one-by-one (normal)
- Gap >= 24 hours: Process in 24-hour batches
- Remaining hours < 24: Process one-by-one
- Optimized for handling long downtime periods
"""

import requests
import json
import uuid
import time
import schedule
from datetime import datetime, timedelta, timezone
from typing import Optional
import logging

# ---------------------------------------------------------------------
# CLICKHOUSE CONFIG
# ---------------------------------------------------------------------
CH_HOST = "ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com"
CH_PORT = 8123
CH_USERNAME = "wm_test"
CH_PASSWORD = "Watermelon@123"
CH_DATABASE = "metrics"
CH_STATE_TABLE = "hourly_pipeline_state"

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# ClickHouse Client
# ---------------------------------------------------------------------
class ClickHouseClient:
    def __init__(self, host, port, username, password, database):
        self.base_url = f"http://{host}:{port}"
        self.auth = (username, password)
        self.database = database

    def execute(self, query: str) -> str:
        r = requests.post(self.base_url, auth=self.auth, data=query, timeout=300)
        r.raise_for_status()
        return r.text.strip()

    def execute_json(self, query: str) -> list:
        query = query.rstrip(";") + " FORMAT JSONEachRow"
        r = requests.post(self.base_url, auth=self.auth, data=query, timeout=300)
        r.raise_for_status()
        if not r.text.strip():
            return []
        return [json.loads(line) for line in r.text.splitlines()]


# ---------------------------------------------------------------------
# ClickHouse DateTime Parser (CRITICAL FIX)
# ---------------------------------------------------------------------
def ch_datetime(value) -> Optional[datetime]:
    """
    Correctly parse ClickHouse DateTime values returned over HTTP JSON.
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return None if value == 0 else datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str) and value.isdigit():
        return None if int(value) == 0 else datetime.fromtimestamp(int(value), tz=timezone.utc)

    try:
        result = datetime.fromisoformat(value)
        return None if result.year < 2000 else result
    except Exception:
        return None


# ---------------------------------------------------------------------
# Hourly Aggregation Pipeline
# ---------------------------------------------------------------------
class HourlyAggregationPipeline:
    def __init__(self, client: ClickHouseClient):
        self.client = client

    # ---------------------------------------------------------------
    # Table
    # ---------------------------------------------------------------
    def ensure_hourly_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.client.database}.ai_service_features_hourly (
            application_id UInt32,
            service_id UInt64,
            project_id Int64,
            service String,
            metric String,
            ts_hour DateTime,

            success_rate_avg Float64,
            success_rate_p50 Float64,
            success_rate_p95 Float64,

            total_windows UInt16,
            bad_windows UInt16,
            breach_ratio Float64,
            slo_target Float64,

            day_of_week UInt8,
            hour UInt8,

            total_requests UInt64,
            response_breach_count Nullable(UInt64),
            avg_latency Nullable(Float64),
            p90_latency Nullable(Float64),

            success_rate_p25 Float64,
            success_rate_p75 Float64,

            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toYYYYMM(ts_hour)
        ORDER BY (application_id, service_id, service, metric, ts_hour)
        """
        self.client.execute(query)

    def ensure_state_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.client.database}.{CH_STATE_TABLE} (
            run_id                         String,
            started_at                     DateTime,
            finished_at                    DateTime,
            status                         LowCardinality(String),
            source_latest_safe_hour        Nullable(DateTime),
            last_processed_hour_before_run Nullable(DateTime),
            first_hour_processed           Nullable(DateTime),
            last_hour_processed            Nullable(DateTime),
            total_hours_processed          UInt32,
            batch_mode                     UInt8,
            batch_count                    UInt16,
            duration_seconds               Float32
        )
        ENGINE = MergeTree()
        ORDER BY started_at
        """
        self.client.execute(query)

    # ---------------------------------------------------------------
    # Bounds
    # ---------------------------------------------------------------
    def get_latest_safe_hour_from_5min(self) -> Optional[datetime]:
        query = f"""
        SELECT toStartOfHour(max(ts)) - INTERVAL 1 HOUR AS hour
        FROM {self.client.database}.ai_metrics_5m_v2
        """
        rows = self.client.execute_json(query)
        return ch_datetime(rows[0].get("hour")) if rows else None

    def get_latest_hourly_hour(self) -> Optional[datetime]:
        query = f"""
        SELECT max(ts_hour) AS hour
        FROM (
            SELECT ts_hour
            FROM {self.client.database}.ai_service_features_hourly
            GROUP BY ts_hour
            HAVING COUNT(DISTINCT metric) = 2
        )
        """
        rows = self.client.execute_json(query)
        return ch_datetime(rows[0].get("hour")) if rows else None

    def get_earliest_hour_from_5min(self) -> Optional[datetime]:
        query = f"""
        SELECT min(toStartOfHour(ts)) AS hour
        FROM {self.client.database}.ai_metrics_5m_v2
        """
        rows = self.client.execute_json(query)
        return ch_datetime(rows[0].get("hour")) if rows else None

    # ---------------------------------------------------------------
    # Aggregations
    # ---------------------------------------------------------------
    def aggregate_success_rate(self, start: datetime, end: datetime):
        query = f"""
        INSERT INTO {self.client.database}.ai_service_features_hourly
        SELECT
            application_id,
            service_id,
            project_id,
            service,
            'success_rate',
            toStartOfHour(ts),

            AVG(success_rate),
            quantile(0.5)(success_rate),
            quantile(0.95)(success_rate),

            COUNT(*),
            countIf(success_rate < success_target),
            countIf(success_rate < success_target) / nullIf(COUNT(*), 0),
            MAX(success_target),

            toDayOfWeek(toStartOfHour(ts)),
            toHour(toStartOfHour(ts)),

            SUM(total_count),
            NULL,
            NULL,
            NULL,

            quantile(0.25)(success_rate),
            quantile(0.75)(success_rate),

            now()
        FROM {self.client.database}.ai_metrics_5m_v2
        WHERE ts >= '{start:%Y-%m-%d %H:%M:%S}'
          AND ts <  '{end:%Y-%m-%d %H:%M:%S}'
        GROUP BY application_id, service_id, project_id, service, toStartOfHour(ts)
        """
        self.client.execute(query)

    def aggregate_latency(self, start: datetime, end: datetime):
        query = f"""
        INSERT INTO {self.client.database}.ai_service_features_hourly
        SELECT
            application_id,
            service_id,
            project_id,
            service,
            'latency',
            toStartOfHour(ts),

            AVG(response_success_rate),
            quantile(0.5)(response_success_rate),
            quantile(0.95)(response_success_rate),

            COUNT(*),
            countIf(response_success_rate < response_target_percent),
            countIf(response_success_rate < response_target_percent) / nullIf(COUNT(*), 0),
            MAX(response_target_percent),

            toDayOfWeek(toStartOfHour(ts)),
            toHour(toStartOfHour(ts)),

            SUM(total_count),
            SUM(response_breach_count),
            SUM(sum_response_time) / nullIf(SUM(total_count), 0),
            SUM(p90_latency * total_count) / nullIf(SUM(total_count), 0),

            quantile(0.25)(response_success_rate),
            quantile(0.75)(response_success_rate),

            now()
        FROM {self.client.database}.ai_metrics_5m_v2
        WHERE ts >= '{start:%Y-%m-%d %H:%M:%S}'
          AND ts <  '{end:%Y-%m-%d %H:%M:%S}'
        GROUP BY application_id, service_id, project_id, service, toStartOfHour(ts)
        """
        self.client.execute(query)

    # ---------------------------------------------------------------
    # Run (with 24-hour batch processing)
    # ---------------------------------------------------------------
    def run(self):
        run_start = time.time()
        started_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        self.ensure_hourly_table()
        self.ensure_state_table()

        latest_safe = self.get_latest_safe_hour_from_5min()
        last_hourly = self.get_latest_hourly_hour()
        earliest_source = self.get_earliest_hour_from_5min()

        state = {
            "run_id": str(uuid.uuid4()),
            "started_at": started_at,
            "status": "noop",
            "source_latest_safe_hour": latest_safe,
            "last_processed_before_run": last_hourly,
            "first_hour_processed": None,
            "last_hour_processed": None,
            "total_hours_processed": 0,
            "batch_mode": 0,
            "batch_count": 0,
        }

        if not latest_safe or not earliest_source:
            logger.info("No usable data in 5-min table yet")
            self._save_state(state, run_start)
            return

        start_hour = last_hourly + timedelta(hours=1) if last_hourly else earliest_source
        start_hour = max(start_hour, earliest_source)
        end_hour = latest_safe

        if start_hour > end_hour:
            logger.info("Hourly table already up to date")
            state["status"] = "up_to_date"
            self._save_state(state, run_start)
            return

        total_hours = int((end_hour - start_hour).total_seconds() / 3600) + 1
        logger.info(f"Processing {total_hours} hours: {start_hour} → {end_hour}")

        state["first_hour_processed"] = start_hour
        current = start_hour

        # Strategy: If gap >= 24 hours, use batch processing
        if total_hours >= 24:
            state["batch_mode"] = 1
            logger.info(f"Using batch processing (24-hour batches)")

            while current <= end_hour:
                hours_remaining = int((end_hour - current).total_seconds() / 3600) + 1

                if hours_remaining >= 24:
                    batch_end = current + timedelta(hours=24)
                    logger.info(f"Processing batch: {current} → {batch_end - timedelta(hours=1)} (24 hours)")
                    self.aggregate_success_rate(current, batch_end)
                    self.aggregate_latency(current, batch_end)
                    state["batch_count"] += 1
                    state["total_hours_processed"] += 24
                    state["last_hour_processed"] = batch_end - timedelta(hours=1)
                    current = batch_end
                else:
                    logger.info(f"Processing remaining {hours_remaining} hours one-by-one")
                    while current <= end_hour:
                        next_hour = current + timedelta(hours=1)
                        logger.info(f"Processing hour {current}")
                        self.aggregate_success_rate(current, next_hour)
                        self.aggregate_latency(current, next_hour)
                        state["total_hours_processed"] += 1
                        state["last_hour_processed"] = current
                        current = next_hour
                    break
        else:
            logger.info(f"Processing {total_hours} hours one-by-one (gap < 24 hours)")
            while current <= end_hour:
                next_hour = current + timedelta(hours=1)
                logger.info(f"Processing hour {current}")
                self.aggregate_success_rate(current, next_hour)
                self.aggregate_latency(current, next_hour)
                state["total_hours_processed"] += 1
                state["last_hour_processed"] = current
                current = next_hour

        state["status"] = "success"
        self._save_state(state, run_start)
        logger.info("Hourly aggregation completed successfully")

    def _save_state(self, state: dict, run_start: float):
        duration = round(time.time() - run_start, 2)

        def fmt(val):
            if val is None:
                return 'NULL'
            if isinstance(val, datetime):
                return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            return f"'{val}'"

        query = f"""
        INSERT INTO {self.client.database}.{CH_STATE_TABLE}
        (run_id, started_at, finished_at, status,
         source_latest_safe_hour, last_processed_hour_before_run,
         first_hour_processed, last_hour_processed,
         total_hours_processed, batch_mode, batch_count, duration_seconds)
        VALUES (
            '{state["run_id"]}',
            '{state["started_at"]}',
            now(),
            '{state["status"]}',
            {fmt(state.get("source_latest_safe_hour"))},
            {fmt(state.get("last_processed_before_run"))},
            {fmt(state.get("first_hour_processed"))},
            {fmt(state.get("last_hour_processed"))},
            {state.get("total_hours_processed", 0)},
            {state.get("batch_mode", 0)},
            {state.get("batch_count", 0)},
            {duration}
        )
        """
        self.client.execute(query)


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------
if __name__ == "__main__":
    client = ClickHouseClient(
        CH_HOST, CH_PORT, CH_USERNAME, CH_PASSWORD, CH_DATABASE
    )
    pipeline = HourlyAggregationPipeline(client)

    def job():
        try:
            pipeline.run()
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

    # Run immediately on start, then every hour at :25
    job()
    schedule.every().hour.at(":25").do(job)

    while True:
        schedule.run_pending()
        time.sleep(30)
