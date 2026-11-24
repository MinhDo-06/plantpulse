import asyncio
import json
import logging
import logging.config
import os
import time
from contextlib import asynccontextmanager

import connexion
import yaml
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from connexion import NoContent, request
from connexion.lifecycle import ConnexionResponse

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC_PRECIP = os.getenv("KAFKA_TOPIC_PRECIP", "precipitation")
KAFKA_TOPIC_SOIL = os.getenv("KAFKA_TOPIC_SOIL", "soil-moisture")

with open("log_conf.yml") as f:
    logging.config.dictConfig(yaml.safe_load(f))
logger = logging.getLogger("receiver")


@asynccontextmanager
async def lifespan(app):
    producer = AIOKafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Producer ID + sequence numbers prevent broker-side duplicates
        enable_idempotence=True,
        # Upper bound on a broker round-trip attempt
        request_timeout_ms=10_000,
        retry_backoff_ms=250,
        linger_ms=5,
    )
    try:
        await producer.start()
    except KafkaError as e:
        logger.critical("Kafka unavailable, refusing to start: %s", e)
        await producer.stop()
        os._exit(1)  # sys.exit here would make the server print a stack trace
    try:
        yield{"kafka": producer}
    finally:
        await producer.stop()


async def report_precipitation(body):
    body["trace_id"] = time.time_ns()
    logger.info("Received 'precipitation' event - rainfall %s mm | trace_id: %s", body["rainfall_mm"], body["trace_id"]) 
    producer = request.state.kafka
    payload = json.dumps(body).encode("utf-8")
    try:
        await asyncio.wait_for(producer.send_and_wait(KAFKA_TOPIC_PRECIP, payload), timeout=5)
    except (KafkaError, asyncio.TimeoutError) as e:
        logger.error("Kafka unavailable, rejecting 'precipitation' event: %r | trace_id: %s", e, body["trace_id"])
        return {"error": "temporarily unavailable"}, 503
    return NoContent, 201

async def report_soil_moisture(body):
    body["trace_id"] = time.time_ns()
    logger.info("Received 'soil moisture' event - moisture: %s%% | trace_id: %s", body["moisture_percent"], body["trace_id"])
    producer = request.state.kafka
    payload = json.dumps(body).encode("utf-8")
    try:
        await asyncio.wait_for(producer.send_and_wait(KAFKA_TOPIC_SOIL, payload), timeout=5)
    except (KafkaError, asyncio.TimeoutError) as e:
        logger.error("Kafka unavailable, rejecting 'soil moisture' event: %r | trace_id: %s", e, body["trace_id"])
        return {"error": "temporarily unavailable"}, 503
    return NoContent, 201


async def get_status():
    return NoContent, 200


def internal_error(request, exc):
    logger.exception("Unhandled error", exc_info=exc)
    return ConnexionResponse(status_code=500, mimetype="application/json",
                             body=json.dumps({"error": "internal error"}))


app = connexion.AsyncApp(__name__, specification_dir='', lifespan=lifespan)
app.add_api("weather.yaml", strict_validation=True, validate_responses=True)
app.add_error_handler(Exception, internal_error)
