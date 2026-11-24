import asyncio
import json
import logging
import logging.config
import os
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from connexion.lifecycle import ConnexionResponse

import connexion
import httpx
import yaml
from connexion import NoContent

STORAGE_URL = os.getenv("STORAGE_URL", "http://localhost:8090")
STATS_PATH = Path(os.getenv("STATS_PATH", "/data/statistics.json"))
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "10"))
# storage stamps recorded_timestamp before its commit, so a row with an older stamp can
# appear after a newer one; only events older than this are counted, the rest wait a round
SETTLE_SECONDS = float(os.getenv("SETTLE_SECONDS", "2"))
EPOCH = "1970-01-01T00:00:00+00:00"

with open("log_conf.yml") as f:
    logging.config.dictConfig(yaml.safe_load(f))
logger = logging.getLogger("processing")


def default_stats():
    return {
        "num_precipitation_readings": 0,
        "num_soil_moisture_readings": 0,
        "max_rainfall_mm": 0.0,
        "avg_moisture_percent": 0.0,
        "last_updated_precipitation": EPOCH,
        "last_updated_soil_moisture": EPOCH,
    }


def load_stats():
    if STATS_PATH.exists():
        return json.loads(STATS_PATH.read_text())
    return default_stats()


def save_stats(stats):
    # write to a temp file then swap, so get_stats never reads a half-written file
    STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = STATS_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(stats))
    temp.replace(STATS_PATH)


def apply_events(stats, precipitation, soil_moisture):
    # storage returns events ordered by recorded_timestamp, so [-1] is the latest
    if precipitation:
        stats["num_precipitation_readings"] += len(precipitation)
        stats["max_rainfall_mm"] = max(
            stats["max_rainfall_mm"], max(e["rainfall_mm"] for e in precipitation)
        )
        stats["last_updated_precipitation"] = precipitation[-1]["recorded_timestamp"]
    if soil_moisture:
        old_count = stats["num_soil_moisture_readings"]
        total = stats["avg_moisture_percent"] * old_count + sum(
            e["moisture_percent"] for e in soil_moisture
        )
        stats["num_soil_moisture_readings"] = old_count + len(soil_moisture)
        stats["avg_moisture_percent"] = total / stats["num_soil_moisture_readings"]
        stats["last_updated_soil_moisture"] = soil_moisture[-1]["recorded_timestamp"]


def settled(events, cutoff):
    return [e for e in events if datetime.fromisoformat(e["recorded_timestamp"]) < cutoff]


async def fetch_events(client, event_type, after):
    response = await client.get(f"{STORAGE_URL}/sensor/{event_type}", params={"after": after})
    response.raise_for_status()
    return response.json()


async def run_once(client):
    start = time.monotonic()
    logger.info("Periodic processing started")
    stats = await asyncio.to_thread(load_stats)
    cutoff = datetime.now(UTC) - timedelta(seconds=SETTLE_SECONDS)
    precipitation, soil_moisture = await asyncio.gather(
        fetch_events(client, "precipitation", stats["last_updated_precipitation"]),
        fetch_events(client, "soil-moisture", stats["last_updated_soil_moisture"]),
    )
    precipitation = settled(precipitation, cutoff)
    soil_moisture = settled(soil_moisture, cutoff)
    logger.info(
        "Received %d precipitation and %d soil moisture events from storage",
        len(precipitation), len(soil_moisture),
    )
    if precipitation or soil_moisture:
        apply_events(stats, precipitation, soil_moisture)
        await asyncio.to_thread(save_stats, stats)
        logger.info("Statistics updated: %s", stats)
    else:
        logger.info("No new events, statistics unchanged")
    logger.info("Processing finished in %d ms", (time.monotonic() - start) * 1000)


async def update_loop(client):
    while True:
        try:
            await run_once(client)
        except httpx.HTTPError as e:
            logger.error("Storage service not accessible or request failed: %r", e)
        except Exception:
            logger.exception("Unexpected error during processing")
        await asyncio.sleep(INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app):
    client = httpx.AsyncClient(timeout=7.0)
    try:
        (await client.get(f"{STORAGE_URL}/status")).raise_for_status()
    except httpx.HTTPError as e:
        logger.critical("Storage unavailable, refusing to start: %r", e)
        await client.aclose()
        os._exit(1)  # sys.exit here would make the server print a stack trace
    task = asyncio.create_task(update_loop(client))
    logger.info("Background processing started, polling %s every %ds", STORAGE_URL, INTERVAL_SECONDS)
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await client.aclose()
        logger.info("Background processing stopped")


def get_stats():
    return load_stats(), 200


def get_status():
    return NoContent, 200


def internal_error(request, exc):
    logger.exception("Unhandled error", exc_info=exc)
    return ConnexionResponse(status_code=500, mimetype="application/json",
                             body=json.dumps({"error": "internal error"}))


app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("processing.yaml", strict_validation=True, validate_responses=True)
app.add_error_handler(Exception, internal_error)
