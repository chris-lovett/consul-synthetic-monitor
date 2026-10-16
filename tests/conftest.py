import httpx
import pytest

from synthmon.app import create_app
from synthmon.config import Config


def make_cfg(cluster: str, nxt: str | None = None, ring: list[str] | None = None, **kw) -> Config:
    data = {
        "cluster": cluster,
        "chain": {
            "enabled": True,
            "expected_clusters": ring or [],
            "next": {"name": nxt, "url": f"http://{nxt}"} if nxt else None,
        },
        **kw,
    }
    return Config.model_validate(data)


class Mesh:
    """In-process mesh: hostnames route to ASGI apps; individual hosts can be broken."""

    def __init__(self, names: list[str], broken: dict[str, str] | None = None, ring: bool = True):
        self.broken = broken or {}
        self.calls: list[str] = []
        self.apps = {}
        for i, n in enumerate(names):
            nxt = names[(i + 1) % len(names)] if ring or i + 1 < len(names) else None
            cfg = make_cfg(n, nxt, ring=names)
            app = create_app(cfg, start_monitor=False)
            self.apps[n] = app
        self.asgi = {n: httpx.ASGITransport(app=a) for n, a in self.apps.items()}
        for n, app in self.apps.items():
            app.state.monitor.http = self.client()

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._route))

    async def _route(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        self.calls.append(host)
        mode = self.broken.get(host)
        if mode == "connect":
            raise httpx.ConnectError("connection refused")
        if mode == "timeout":
            raise httpx.ReadTimeout("timed out")
        if mode == "reset":
            raise httpx.RemoteProtocolError("server disconnected without sending a response")
        if mode == "403":
            return httpx.Response(403, text="RBAC: access denied")
        if mode == "500":
            return httpx.Response(500, text="boom")
        if mode == "badjson":
            return httpx.Response(200, text="<html>")
        if mode and mode.startswith("impostor:"):
            return httpx.Response(200, json={"cluster": mode.split(":", 1)[1]})
        return await self.asgi[host].handle_async_request(request)


@pytest.fixture
def mesh_factory():
    return Mesh
