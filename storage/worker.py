import json
import logging
import logging.config
import os
import signal
from datetime import UTC, datetime
import time
from sqlalchemy.exc import IntegrityError, OperationalError

import yaml
from confluent_kafka import Consumer, KafkaException

from db import ENGINE, make_session
from models import Base, PrecipitationReading, SoilMoistureReading

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC_PRECIP = os.getenv("KAFKA_TOPIC_PRECIP", "precipitation")
TOPIC_SOIL = os.getenv("KAFKA_TOPIC_SOIL", "soil-moisture")

with open("log_conf.yml") as f:
    logging.config.dictConfig(yaml.safe_load(f))
logger =  logging.getLogger("storage-worker")

def handle_sigterm(signum, frame):
    # Docker stops, sends SIGTERM, turns into Ctrl-C so the finally block runs
    raise KeyboardInterrupt

def save_event(session, topic, value):
    ts = datetime.fromisoformat(value["timestamp"]).astimezone(UTC)
    if topic == TOPIC_PRECIP:
        event  = PrecipitationReading(
            sensor_id = value["sensor_id"],
            trace_id = value["trace_id"],
            garden_zone = value["garden_zone"],
            rainfall_mm = value["rainfall_mm"],
            timestamp=ts,
        )
    elif topic == TOPIC_SOIL:
        event =  SoilMoistureReading(
            sensor_id = value["sensor_id"],
            trace_id = value["trace_id"],
            garden_zone = value["garden_zone"],
            moisture_percent = value["moisture_percent"],
            timestamp=ts,
        )
    else:
        raise ValueError(f"unknown topic {topic}")
    session.add(event)
    session.commit()
    logger.debug("Stored event from '%s' with trace id of %s", topic, value["trace_id"])

MAX_ATTEMPS = 5

def save_with_retry(topic, value):
    "Fresh session per new attemp"
    for attempt in range(1, MAX_ATTEMPS + 1):
        session = make_session()
        try:
            save_event(session, topic, value)
            return
        except OperationalError:
            session.rollback()
            if attempt == MAX_ATTEMPS:
                raise
            delay = min(2 ** attempt, 30)
            logger.warning("DB unavailable (attempt %d/%d), retrying in %ds", attempt, MAX_ATTEMPS, delay)
            time.sleep(delay)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    Base.metadata.create_all(ENGINE)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "storage_worker_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,

    })

    consumer.subscribe([TOPIC_PRECIP, TOPIC_SOIL])
    logger.info("Storage worker listening on %s, %s", TOPIC_PRECIP, TOPIC_SOIL)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                logger.warning("Kafka error: %s", msg.error())
                continue

            try: 
                value = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                logger.error("Skipping malformed message at %s[%s]@%s",
                             msg.topic(), msg.partition(), msg.offset())
                consumer.commit(msg)
                continue

            try:
                save_with_retry(msg.topic(), value)
            except KeyError as e:
                logger.error("Skipping message missing field %s at offset %s", e, msg.offset())
            except IntegrityError:
                logger.warning("Duplicate trace_id %s at %s[%s]@%s, skipping",
                               value.get("trace_id"), msg.topic(), msg.partition(), msg.offset())
            # anything else (DB down after 5 attempts) propagates: no commit, restart replays
            consumer.commit(msg)
    except KafkaException as ke:
        logger.error("Kafka problem: %s", ke)
        raise
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        logger.info("Closing Kafka connection...")
        consumer.close()

if __name__ == "__main__":
    main()