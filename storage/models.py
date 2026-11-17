from datetime import datetime, UTC

from sqlalchemy import String, Numeric, BigInteger
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PrecipitationReading(Base):
    __tablename__ = "precipitation"

    id: Mapped[int] = mapped_column(primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(50))
    trace_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    garden_zone: Mapped[str] = mapped_column(String(50))
    rainfall_mm: Mapped[float] = mapped_column(Numeric(8, 2))
    timestamp: Mapped[datetime] = mapped_column(DATETIME(fsp=6))
    recorded_timestamp: Mapped[datetime] = mapped_column(DATETIME(fsp=6), default=lambda: datetime.now(UTC))

    def to_json(self):
        return {
            "sensor_id": self.sensor_id,
            "trace_id": self.trace_id,
            "garden_zone": self.garden_zone,
            "rainfall_mm": float(self.rainfall_mm),
            "timestamp": self.timestamp.replace(tzinfo=UTC).isoformat(),
            "recorded_timestamp": self.recorded_timestamp.replace(tzinfo=UTC).isoformat()
        }


class SoilMoistureReading(Base):
    __tablename__ = "soil_moisture"

    id: Mapped[int] = mapped_column(primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(50))
    trace_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    garden_zone: Mapped[str] = mapped_column(String(50))
    moisture_percent: Mapped[float] = mapped_column(Numeric(8, 2))
    timestamp: Mapped[datetime] = mapped_column(DATETIME(fsp=6))
    recorded_timestamp: Mapped[datetime] = mapped_column(DATETIME(fsp=6), default=lambda: datetime.now(UTC))

    def to_json(self):
        return {
            "sensor_id": self.sensor_id,
            "trace_id": self.trace_id,
            "garden_zone": self.garden_zone,
            "moisture_percent": float(self.moisture_percent),
            "timestamp": self.timestamp.replace(tzinfo=UTC).isoformat(),
            "recorded_timestamp": self.recorded_timestamp.replace(tzinfo=UTC).isoformat(),
        }
