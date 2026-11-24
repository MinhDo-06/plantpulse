import json
import os

os.environ["DATABASE_URL"] = "mysql+pymysql://u:p@localhost/x"  # never connected to

import pytest
from confluent_kafka import KafkaError, KafkaException
from sqlalchemy.exc import IntegrityError, OperationalError

import worker

GOOD = json.dumps({"trace_id": 1}).encode()


class FakeMsg:
    def __init__(self, raw):
        self.raw = raw

    def error(self): return None
    def value(self): return self.raw
    def topic(self): return worker.TOPIC_PRECIP
    def partition(self): return 0
    def offset(self): return 7


class FakeConsumer:
    """Serves the given messages, then stops the worker like SIGTERM would."""

    def __init__(self, messages=(), list_error=None, commit_error=None):
        self.messages = list(messages)
        self.list_error, self.commit_error = list_error, commit_error
        self.commits, self.subscribed, self.closed = [], False, False

    def list_topics(self, timeout):
        if self.list_error:
            raise self.list_error

    def subscribe(self, topics): self.subscribed = True
    def close(self): self.closed = True

    def poll(self, timeout):
        if not self.messages:
            raise KeyboardInterrupt
        return self.messages.pop(0)

    def commit(self, message=None, asynchronous=True):
        if self.commit_error:
            raise self.commit_error
        self.commits.append((message, asynchronous))


def db_error(cls, code):
    return cls("stmt", {}, Exception(code, "boom"))


def raiser(exc):
    def f(*args, **kwargs):
        raise exc
    return f


KAFKA_DOWN = KafkaException(KafkaError(KafkaError._TRANSPORT))


@pytest.fixture
def run(monkeypatch):
    def _run(consumer, save=lambda topic, value: None, create_all=lambda engine: None):
        monkeypatch.setattr(worker, "Consumer", lambda config: consumer)
        monkeypatch.setattr(worker, "save_with_retry", save)
        monkeypatch.setattr(worker.Base.metadata, "create_all", create_all)
        monkeypatch.setattr(worker.signal, "signal", lambda *a: None)
        worker.main()
        return consumer
    return _run


@pytest.mark.parametrize("raw", [GOOD, b"not json", None])
def test_every_commit_is_synchronous(run, raw):
    msg = FakeMsg(raw)
    consumer = run(FakeConsumer([msg]))
    assert consumer.commits == [(msg, False)]
    assert consumer.closed


def test_duplicate_is_logged_as_duplicate_and_committed(run, caplog):
    consumer = run(FakeConsumer([FakeMsg(GOOD)]), save=raiser(db_error(IntegrityError, 1062)))
    assert "Duplicate trace_id 1" in caplog.text
    assert len(consumer.commits) == 1


def test_other_integrity_error_is_not_called_duplicate(run, caplog):
    consumer = run(FakeConsumer([FakeMsg(GOOD)]), save=raiser(db_error(IntegrityError, 1048)))
    assert "Duplicate" not in caplog.text
    assert any(r.levelname == "ERROR" and "1048" in r.getMessage() for r in caplog.records)
    assert len(consumer.commits) == 1


def test_db_down_exits_without_commit_or_trace(run, caplog):
    consumer = FakeConsumer([FakeMsg(GOOD)])
    with pytest.raises(SystemExit) as e:
        run(consumer, save=raiser(db_error(OperationalError, 2003)))
    assert e.value.code == 1
    assert consumer.commits == [] and consumer.closed
    assert "Database unavailable" in caplog.text
    assert not any(r.exc_info for r in caplog.records)


@pytest.mark.parametrize("raw", [GOOD, b"not json"])
def test_commit_failure_exits_without_trace(run, caplog, raw):
    consumer = FakeConsumer([FakeMsg(raw)], commit_error=KAFKA_DOWN)
    with pytest.raises(SystemExit) as e:
        run(consumer)
    assert e.value.code == 1 and consumer.closed
    assert "Kafka problem" in caplog.text
    assert not any(r.exc_info for r in caplog.records)


def test_refuses_to_start_without_kafka(run, caplog):
    consumer = FakeConsumer(list_error=KAFKA_DOWN)
    with pytest.raises(SystemExit) as e:
        run(consumer)
    assert e.value.code == 1
    assert not consumer.subscribed and consumer.closed
    assert "refusing to start" in caplog.text


def test_refuses_to_start_without_db(run, caplog):
    consumer = FakeConsumer()
    with pytest.raises(SystemExit) as e:
        run(consumer, create_all=raiser(db_error(OperationalError, 2003)))
    assert e.value.code == 1
    assert not consumer.subscribed
    assert "refusing to start" in caplog.text
