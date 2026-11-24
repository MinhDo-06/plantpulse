import asyncio

import httpx
import pytest

import processing


class Exited(Exception):
    pass


def start(monkeypatch, handler):
    def fake_exit(code):
        raise Exited(code)

    async def idle_loop(client):
        await asyncio.sleep(60)

    real_client = httpx.AsyncClient
    monkeypatch.setattr(processing.httpx, "AsyncClient",
                        lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw))
    monkeypatch.setattr(processing.os, "_exit", fake_exit)
    monkeypatch.setattr(processing, "update_loop", idle_loop)

    async def go():
        async with processing.lifespan(None):
            return True
    return asyncio.run(go())


def refuse(request):
    raise httpx.ConnectError("down")


@pytest.mark.parametrize("handler", [refuse, lambda request: httpx.Response(503)])
def test_refuses_to_start_without_storage(monkeypatch, caplog, handler):
    with pytest.raises(Exited) as e:
        start(monkeypatch, handler)
    assert e.value.args == (1,)
    assert "refusing to start" in caplog.text


def test_starts_with_storage(monkeypatch):
    assert start(monkeypatch, lambda request: httpx.Response(200 if request.url.path == "/status" else 404))
