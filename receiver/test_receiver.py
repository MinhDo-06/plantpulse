import asyncio
from types import SimpleNamespace

import pytest
from aiokafka.errors import KafkaConnectionError

import receiver


class FakeProducer:
    def __init__(self, error=None):
        self.error, self.stopped = error, False

    async def start(self):
        if self.error:
            raise self.error

    async def stop(self): self.stopped = True

    async def send_and_wait(self, topic, payload):
        if self.error:
            raise self.error


class Exited(Exception):
    pass


def start(monkeypatch, producer):
    def fake_exit(code):
        raise Exited(code)

    monkeypatch.setattr(receiver, "AIOKafkaProducer", lambda **kw: producer)
    monkeypatch.setattr(receiver.os, "_exit", fake_exit)

    async def go():
        async with receiver.lifespan(None) as state:
            assert not producer.stopped
            return state
    return asyncio.run(go())


def test_refuses_to_start_without_kafka(monkeypatch, caplog):
    producer = FakeProducer(KafkaConnectionError("down"))
    with pytest.raises(Exited) as e:
        start(monkeypatch, producer)
    assert e.value.args == (1,) and producer.stopped
    assert "refusing to start" in caplog.text


def test_starts_with_kafka(monkeypatch):
    producer = FakeProducer()
    assert start(monkeypatch, producer) == {"kafka": producer}
    assert producer.stopped


HANDLERS = [(receiver.report_precipitation, {"rainfall_mm": 1}),
            (receiver.report_soil_moisture, {"moisture_percent": 1})]


@pytest.mark.parametrize("handler,body", HANDLERS)
@pytest.mark.parametrize("error,code", [(None, 201), (KafkaConnectionError("down"), 503),
                                        (asyncio.TimeoutError(), 503)])
def test_post(monkeypatch, caplog, handler, body, error, code):
    request = SimpleNamespace(state=SimpleNamespace(kafka=FakeProducer(error)))
    monkeypatch.setattr(receiver, "request", request)
    assert asyncio.run(handler(dict(body)))[1] == code
    assert ("Kafka unavailable" in caplog.text) == (code == 503)
    assert not any(r.exc_info for r in caplog.records)
