from datetime import datetime, UTC
from sqlalchemy import select
from db import ENGINE
from models import Base
import connexion
from connexion import NoContent
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import functools
import sys
from connexion.lifecycle import ConnexionResponse
import json

from db import use_db_session
from models import PrecipitationReading, SoilMoistureReading
import logging, logging.config, yaml

with open("log_conf.yml") as f:
    logging.config.dictConfig(yaml.safe_load(f))
logger = logging.getLogger("storage")

def parse_after(after):
    return datetime.fromisoformat(after).astimezone(UTC).replace(tzinfo=None)

def db_503(func):
    @functools.wraps(func)
    def wrapper (*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SQLAlchemyError as e:
            logger.error("Database unavailable in %s: %s", func.__name__, getattr(e, "orig", e))
            return {"error": "temporarily unavailable"}, 503
    return wrapper

@db_503
@use_db_session
def get_precipitation_readings(sessions, after=None):
    query = select(PrecipitationReading).order_by(PrecipitationReading.recorded_timestamp)
    if after:
        query = query.where(PrecipitationReading.recorded_timestamp > parse_after(after))
    readings = sessions.scalars(query).all()
    logger.debug("Found %d precipitation readings after %s", len(readings), after)
    return [r.to_json() for r in readings], 200

@db_503
@use_db_session
def get_soil_moisture_readings(sessions, after=None):
    query = select(SoilMoistureReading).order_by(SoilMoistureReading.recorded_timestamp)
    if after:
        query = query.where(SoilMoistureReading.recorded_timestamp > parse_after(after))
    readings = sessions.scalars(query).all()
    logger.debug("Found %d soil moisture readings after %s", len(readings), after)
    return [r.to_json() for r in readings], 200

def get_status():
    return NoContent, 200

try:
    Base.metadata.create_all(ENGINE)
except OperationalError as e:
    logger.critical("Database unavailable, refusing to start: %s", e.orig)
    sys.exit(1)

def internal_error(request, exc):
    logger.exception("Unhandled error", exc_info=exc)
    return ConnexionResponse(status_code=500, mimetype="application/json", body=json.dumps({"error": "internal error"}))

app = connexion.AsyncApp(__name__, specification_dir='')
app.add_api("storage.yaml", strict_validation=True, validate_responses=True)
app.add_error_handler(Exception, internal_error)