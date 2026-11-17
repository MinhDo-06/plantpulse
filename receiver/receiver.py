import json
import logging
import logging.config
import os
import time
from contextlib import asynccontextmanager

import connexion
import yaml
from aiokafka import AIOKafkaProducer
from connexion import NoContent, request

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC_PRECIP = os.getenv("KAFKA_TOPIC_PRECIP", "precipitation")
KAFKA_TOPIC_SOIL = os.getenv("KAFKA_TOPIC_SOIL", "soil-moisture")

with open("log_conf.yml") as f:
    logging.config.dictConfig(yaml.safe_load(f))
logger = logging.getLogger("receiver")


@asynccontextmanager
async def lifespan(app):
    producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    await producer.start()
    try:
        yield {"kafka": producer}
    finally:
        await producer.stop()


async def report_precipitation(body):
    body["trace_id"] = time.time_ns()
    logger.info("Received 'precipitation' event - rainfall %s mm | trace_id: %s",
                body["rainfall_mm"], body["trace_id"])
    await request.state.kafka.send(KAFKA_TOPIC_PRECIP, json.dumps(body).encode("utf-8"))
    return NoContent, 201


async def report_soil_moisture(body):
    body["trace_id"] = time.time_ns()
    logger.info("Received 'soil moisture' event - moisture: %s%% | trace_id: %s",
                body["moisture_percent"], body["trace_id"])
    await request.state.kafka.send(KAFKA_TOPIC_SOIL, json.dumps(body).encode("utf-8"))
    return NoContent, 201


async def get_status():
    return NoContent, 200


app = connexion.AsyncApp(__name__, specification_dir='', lifespan=lifespan)
app.add_api("weather.yaml", strict_validation=True, validate_responses=True)
