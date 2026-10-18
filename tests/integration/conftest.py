"""Fixtures for integration tests that exercise the local FastAPI service."""
from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator

import pytest
import uvicorn


@pytest.fixture
def fastapi_backend(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Run FastAPI on an ephemeral port with isolated in-memory state."""
    monkeypatch.setenv("STREAMFORGE_STATE_BACKEND", "in_memory")
    monkeypatch.setenv("STREAMFORGE_RECOVER_STATE_FROM_CHANGELOG", "false")

    from stream_processor import consumer
    from streamforge.main import create_app

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    host, port = listener.getsockname()

    base_url = f"http://{host}:{port}"
    monkeypatch.setattr(consumer, "BACKEND_URL", f"{base_url}/telemetry/ingest")

    server = uvicorn.Server(
        uvicorn.Config(create_app(), log_level="critical", lifespan="on")
    )
    thread = threading.Thread(
        target=server.run,
        kwargs={"sockets": [listener]},
        name="streamforge-test-api",
        daemon=True,
    )

    thread.start()
    deadline = time.monotonic() + 10
    try:
        while not server.started:
            if not thread.is_alive():
                pytest.fail("FastAPI integration-test server stopped during startup")
            if time.monotonic() >= deadline:
                pytest.fail("FastAPI integration-test server did not start within 10 seconds")
            time.sleep(0.05)

        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        if thread.is_alive():
            server.force_exit = True
            thread.join(timeout=1)
