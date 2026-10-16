"""Sidecar (Envoy) checks: certificate expiry, xDS health and DNS resolution timing."""
from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from . import metrics as m
from .config import Config
from .consul import parse_ts


def _soonest_expiry(certs_dump: dict[str, Any], now: float) -> dict[str, float]:
    """Return the soonest seconds-to-expiry for each cert kind in an Envoy /certs response."""
    out: dict[str, float] = {}
    for cert in certs_dump.get("certificates", []):
        for kind, key in (("leaf", "cert_chain"), ("ca", "ca_certs")):
            for c in cert.get(key, []) or []:
                exp = parse_ts(c.get("expiration_time"))
                if exp is None:
                    continue
                remaining = exp - now
                out[kind] = min(out.get(kind, remaining), remaining)
    return out


def parse_xds_stats(text: str) -> dict[str, dict[str, float]]:
    """Pull xDS update counters from /stats?format=text output.

    Envoy emits e.g. ``cluster_manager.cds.update_rejected: 0``, ``listener_manager.lds.update_success``
    and ``control_plane.connected_state: 1``.
    """
    stats: dict[str, dict[str, float]] = {"rejected": {}, "failure": {}, "success": {}, "connected": {}}
    for line in text.splitlines():
        if ":" not in line:
            continue
        name, _, raw = line.partition(":")
        name = name.strip()
        try:
            val = float(raw.strip())
        except ValueError:
            continue
        if name == "control_plane.connected_state":
            stats["connected"]["all"] = val
            continue
        parts = name.split(".")
        if len(parts) < 3 or parts[1] not in ("cds", "lds", "rds", "eds", "sds"):
            continue
        if parts[0] not in ("cluster_manager", "listener_manager", "http", "cluster"):
            continue
        xds, stat = parts[1], parts[-1]
        bucket = {"update_rejected": "rejected", "update_failure": "failure", "update_success": "success"}.get(stat)
        if bucket:
            stats[bucket][xds] = stats[bucket].get(xds, 0) + val
    return stats


_last_success: dict[str, tuple[float, float]] = {}  # xds -> (count, time it last advanced)


async def check_envoy(cfg: Config, client: httpx.AsyncClient | None = None) -> None:
    base = cfg.envoy.admin_addr.rstrip("/")
    own = client is None
    client = client or httpx.AsyncClient(timeout=cfg.envoy.timeout_seconds)
    try:
        ready = await client.get(f"{base}/ready")
        m.ENVOY_LIVE.set(1 if ready.status_code == 200 and "LIVE" in ready.text.upper() else 0)

        server = await client.get(f"{base}/server_info")
        if server.status_code == 200:
            # Envoy reports "<commit>/<version>/<build>/<type>/<ssl>".
            parts = str(server.json().get("version", "")).split("/")
            ver = parts[1] if len(parts) > 1 else parts[0]
            if ver:
                m.ENVOY_INFO.labels(ver).set(1)

        certs = await client.get(f"{base}/certs")
        certs.raise_for_status()
        for kind, remaining in _soonest_expiry(certs.json(), time.time()).items():
            m.ENVOY_CERT_EXPIRY.labels(kind).set(remaining)

        stats_resp = await client.get(f"{base}/stats", params={"filter": "(update_|control_plane)"})
        stats_resp.raise_for_status()
        stats = parse_xds_stats(stats_resp.text)
        if stats["connected"]:
            m.ENVOY_XDS_CONNECTED.set(stats["connected"]["all"])
        now = time.time()
        for xds, v in stats["rejected"].items():
            m.ENVOY_XDS_REJECTED.labels(xds).set(v)
        for xds, v in stats["failure"].items():
            m.ENVOY_XDS_FAILURE.labels(xds).set(v)
        for xds, v in stats["success"].items():
            prev = _last_success.get(xds)
            if prev is None or v > prev[0]:
                _last_success[xds] = (v, now)
            m.ENVOY_XDS_AGE.labels(xds).set(now - _last_success[xds][1])
    finally:
        if own:
            await client.aclose()


# --- DNS ---------------------------------------------------------------------

async def check_dns(cfg: Config) -> None:
    """Resolve configured names (Consul DNS or system resolver) and time the lookup."""
    import dns.asyncresolver
    import dns.exception
    import dns.resolver

    resolver = dns.asyncresolver.Resolver(configure=cfg.dns.server is None)
    if cfg.dns.server:
        host, _, port = cfg.dns.server.partition(":")
        resolver.nameservers = [host]
        resolver.port = int(port or 53)
    resolver.lifetime = cfg.dns.timeout_seconds

    async def one(name: str, rtype: str) -> None:
        started = time.perf_counter()
        try:
            answer = await resolver.resolve(name, rtype)
            m.DNS_UP.labels(name, rtype).set(1)
            m.DNS_ANSWERS.labels(name, rtype).set(len(answer))
        except (dns.exception.DNSException, OSError):
            m.DNS_UP.labels(name, rtype).set(0)
            m.DNS_ANSWERS.labels(name, rtype).set(0)
        m.DNS_DURATION.labels(name, rtype).set(time.perf_counter() - started)

    await asyncio.gather(*(one(d.name, d.type) for d in cfg.dns.names))
