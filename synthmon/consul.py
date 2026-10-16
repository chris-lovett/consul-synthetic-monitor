"""Consul control-plane checks (peering, exported services, servers, CA, ACL, license, gateways)."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import httpx

from . import metrics as m
from .config import Config


def parse_ts(value: str | None) -> float | None:
    """Parse an RFC3339 timestamp (Go style, up to nanoseconds) into epoch seconds."""
    if not value or value.startswith("0001-01-01"):
        return None
    v = value.replace("Z", "+00:00")
    if "." in v:
        head, rest = v.split(".", 1)
        tz_at = next((i for i, ch in enumerate(rest) if ch in "+-"), len(rest))
        frac = rest[:tz_at][:6].ljust(6, "0")
        v = f"{head}.{frac}{rest[tz_at:]}"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class ConsulClient:
    def __init__(self, cfg: Config, client: httpx.AsyncClient | None = None):
        self.cfg = cfg
        c = cfg.consul
        headers = {}
        if cfg.consul_token:
            headers["X-Consul-Token"] = cfg.consul_token
        self._http = client or httpx.AsyncClient(
            base_url=c.http_addr.rstrip("/"), headers=headers, timeout=c.timeout_seconds,
            verify=c.ca_file or c.verify,
        )

    def _scope(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        p = dict(params or {})
        if self.cfg.consul.partition:
            p["partition"] = self.cfg.consul.partition
        if self.cfg.consul.namespace:
            p["ns"] = self.cfg.consul.namespace
        return p

    async def get(self, path: str, params: dict[str, Any] | None = None, scoped: bool = False) -> Any:
        r = await self._http.get(path, params=self._scope(params) if scoped else params)
        r.raise_for_status()
        return r.json()

    async def put(self, path: str, json: Any = None) -> None:
        r = await self._http.put(path, json=json, params=self._scope())
        r.raise_for_status()

    async def aclose(self) -> None:
        await self._http.aclose()


# --- individual checks -------------------------------------------------------

async def check_peering(cc: ConsulClient, cfg: Config) -> None:
    """Peering state, heartbeat freshness, and exported/imported service drift."""
    peers = await cc.get("/v1/peerings", scoped=True) or []
    by_name = {p["Name"]: p for p in peers}
    expected = {e.name: e for e in cfg.peering.expected}
    now = time.time()

    for name in sorted(set(by_name) | set(expected)):
        p = by_name.get(name)
        state = (p or {}).get("State", "MISSING")
        # Reset all state series for this peer so stale states do not linger.
        for s in ("ACTIVE", "PENDING", "ESTABLISHING", "FAILING", "TERMINATED", "DELETING", "UNDEFINED", "MISSING"):
            m.PEERING_STATE.labels(name, s).set(1 if s == state else 0)
        m.PEERING_UP.labels(name).set(1 if state == "ACTIVE" else 0)
        if p is None:
            continue

        stream = p.get("StreamStatus") or {}
        exported = stream.get("ExportedServices") or []
        imported = stream.get("ImportedServices") or []
        m.PEERING_SERVICES.labels(name, "exported").set(len(exported))
        m.PEERING_SERVICES.labels(name, "imported").set(len(imported))

        hb = parse_ts(stream.get("LastHeartbeat"))
        if hb is not None:
            m.PEERING_HEARTBEAT_AGE.labels(name).set(max(0.0, now - hb))

        exp = expected.get(name)
        if exp:
            for direction, want, have in (
                ("exported", exp.exported, exported), ("imported", exp.imported, imported),
            ):
                for svc in want:
                    m.PEERING_SERVICE_MISSING.labels(name, direction, svc).set(0 if svc in have else 1)


async def check_servers(cc: ConsulClient, cfg: Config) -> None:
    """Raft leader, autopilot health, server version."""
    leader = await cc.get("/v1/status/leader")
    m.RAFT_LEADER.set(1 if leader else 0)

    health = await cc.get("/v1/operator/autopilot/health")
    m.AUTOPILOT_HEALTHY.set(1 if health.get("Healthy") else 0)
    m.AUTOPILOT_FAILURE_TOLERANCE.set(health.get("FailureTolerance", 0))
    for srv in health.get("Servers", []):
        m.SERVER_HEALTHY.labels(srv.get("Name", srv.get("ID", "?"))).set(1 if srv.get("Healthy") else 0)

    try:
        info = await cc.get("/v1/agent/self")
        ver = info.get("Config", {}).get("Version")
        if ver:
            m.CONSUL_INFO.labels(ver).set(1)
    except httpx.HTTPError:
        pass  # version is informational

    for svc in cfg.servers.mesh_gateways:
        entries = await cc.get(f"/v1/health/service/{svc}", params={"passing": "true"}, scoped=True)
        m.GATEWAY_INSTANCES.labels(svc).set(len(entries))
        m.GATEWAY_BELOW_MIN.labels(svc).set(1 if len(entries) < cfg.servers.min_gateway_instances else 0)


async def check_ca_and_license(cc: ConsulClient, cfg: Config) -> None:
    now = time.time()
    roots = await cc.get("/v1/connect/ca/roots")
    items = roots.get("Roots", [])
    m.CA_ROOT_COUNT.set(len(items))
    active = [r for r in items if r.get("Active")]
    if active:
        exp = parse_ts(active[0].get("NotAfter"))
        if exp:
            m.CA_ROOT_EXPIRY.set(exp - now)

    try:
        lic = await cc.get("/v1/operator/license")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (404, 501):
            return  # OSS server: no license endpoint
        raise
    exp = parse_ts((lic.get("License") or {}).get("expiration_time"))
    if exp:
        m.LICENSE_EXPIRY.set(exp - now)


async def check_acl(cc: ConsulClient, cfg: Config) -> None:
    """Verify the probe token still resolves and track its expiry."""
    try:
        tok = await cc.get("/v1/acl/token/self")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403):
            m.ACL_TOKEN_OK.set(0)
            return
        raise
    m.ACL_TOKEN_OK.set(1)
    exp = parse_ts(tok.get("ExpirationTime"))
    if exp:
        m.ACL_TOKEN_EXPIRY.set(exp - time.time())


# --- propagation canary ------------------------------------------------------

async def publish_canary(cc: ConsulClient, cfg: Config) -> None:
    """(Re)register the canary with a fresh timestamp; peers observe it via exported-services.

    Uses the catalog API rather than the agent API so it works with consul-dataplane, where
    there is no local client agent. Needs node:write and service:write on the canary.
    """
    name = cfg.canary.service_name
    body = {
        "Node": f"{name}-{cfg.cluster}",
        "Address": "127.0.0.1",
        "NodeMeta": {"external-node": "true"},
        "Service": {
            "ID": f"{name}-{cfg.cluster}",
            "Service": name,
            "Meta": {"origin": cfg.cluster, "published_at": f"{time.time():.3f}"},
        },
    }
    await cc.put("/v1/catalog/register", json=body)
    m.CANARY_PUBLISH_UP.set(1)


_seen_canary: dict[tuple[str, str], str] = {}


async def observe_canary(cc: ConsulClient, cfg: Config) -> None:
    """Read each peer's canary from the imported catalog and measure age/propagation delay."""
    for peer in cfg.canary.peers:
        try:
            entries = await cc.get(
                f"/v1/health/service/{cfg.canary.service_name}",
                params={"peer": peer, "passing": "true"}, scoped=False,
            )
        except httpx.HTTPError:
            m.CANARY_AGE.labels(peer, cfg.cluster).set(float("inf"))
            raise
        stamps = []
        for e in entries:
            meta = (e.get("Service") or {}).get("Meta") or {}
            if "published_at" in meta:
                stamps.append(meta["published_at"])
        if not stamps:
            m.CANARY_AGE.labels(peer, cfg.cluster).set(float("inf"))
            continue
        newest = max(stamps, key=float)
        now = time.time()
        m.CANARY_AGE.labels(peer, cfg.cluster).set(max(0.0, now - float(newest)))
        key = (peer, cfg.cluster)
        if _seen_canary.get(key) != newest:
            if key in _seen_canary:  # skip the first sighting after restart: publish time is unknown
                m.CANARY_PROPAGATION.labels(peer, cfg.cluster).observe(max(0.0, now - float(newest)))
            _seen_canary[key] = newest
