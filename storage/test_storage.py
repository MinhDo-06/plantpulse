import importlib
import os
import sys

os.environ["DATABASE_URL"] = "mysql+pymysql://u:p@localhost/x"  # never connected to

import pytest
from sqlalchemy.exc import OperationalError

from models import Base


def fail(*args):
    raise OperationalError("stmt", {}, Exception(2003, "boom"))


def load(monkeypatch, create_all=lambda engine: None):
    monkeypatch.setattr(Base.metadata, "create_all", create_all)
    monkeypatch.setattr("logging.config.dictConfig", lambda conf: None)  # would detach caplog
    monkeypatch.delitem(sys.modules, "storage", raising=False)
    return importlib.import_module("storage")


def test_refuses_to_start_without_db(monkeypatch, caplog):
    with pytest.raises(SystemExit) as e:
        load(monkeypatch, fail)
    assert e.value.code == 1
    assert "refusing to start" in caplog.text


def test_db_error_is_503_without_trace(monkeypatch, caplog):
    storage = load(monkeypatch)
    assert storage.db_503(fail)()[1] == 503
    assert "Database unavailable" in caplog.text
    assert not any(r.exc_info for r in caplog.records)


def test_healthy_result_passes_through(monkeypatch):
    storage = load(monkeypatch)
    assert storage.db_503(lambda: ([], 200))() == ([], 200)
