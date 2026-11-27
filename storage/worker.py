import json
import logging
import logging.config
import os
import signal
import sys
from datetime import UTC, datetime
import time
from sqlalchemy.exc import DataError, IntegrityError, OperationalError

import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

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
MYSQL_DUPLICATE_KEY = 1062

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


def ensure_topics():
    "Create the topics if the receiver has not yet, so the consumer group can join them"
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    futures = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1)
                                   for t in (TOPIC_PRECIP, TOPIC_SOIL)])
    for future in futures.values():
        try:
            future.result()
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        Base.metadata.create_all(ENGINE)
    except OperationalError as e:
        logger.critical("Database unavailable, refusing to start: %s", e.orig)
        sys.exit(1)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "storage_worker_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,

    })

    try:
        try:
            consumer.list_topics(timeout=10)
            ensure_topics()
        except KafkaException as e:
            logger.critical("Kafka unavailable, refusing to start: %s", e)
            sys.exit(1)
        consumer.subscribe([TOPIC_PRECIP, TOPIC_SOIL])
        logger.info("Storage worker listening on %s, %s", TOPIC_PRECIP, TOPIC_SOIL)

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
                if not isinstance(value, dict):
                    raise ValueError("not a JSON object")
            except (ValueError, UnicodeDecodeError, AttributeError):  # AttributeError: empty (tombstone) message
                logger.error("Skipping malformed message at %s[%s]@%s",
                             msg.topic(), msg.partition(), msg.offset())
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                save_with_retry(msg.topic(), value)
            except (KeyError, ValueError, TypeError) as e:
                logger.error("Skipping bad message (%r) at %s[%s]@%s",
                             e, msg.topic(), msg.partition(), msg.offset())
            except IntegrityError as e:
                if e.orig and e.orig.args[0] == MYSQL_DUPLICATE_KEY:
                    logger.warning("Duplicate trace_id %s at %s[%s]@%s, skipping",
                                   value.get("trace_id"), msg.topic(), msg.partition(), msg.offset())
                else:
                    logger.error("Skipping message that violates a DB constraint (%s) at %s[%s]@%s",
                                 e.orig, msg.topic(), msg.partition(), msg.offset())
            except DataError as e:
                logger.error("Skipping message with invalid data (%s) at %s[%s]@%s",
                             e.orig, msg.topic(), msg.partition(), msg.offset())
            except OperationalError as e:
                # no commit: Docker restarts the worker and Kafka replays this message
                logger.critical("Database unavailable after %d attempts, exiting: %s", MAX_ATTEMPS, e.orig)
                sys.exit(1)
            consumer.commit(message=msg, asynchronous=False)
    except KafkaException as ke:
        logger.critical("Kafka problem, exiting: %s", ke)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        logger.info("Closing Kafka connection...")
        consumer.close()

if __name__ == "__main__":
    main()