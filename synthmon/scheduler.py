"""Periodic task runner with jitter and self-observability."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Awaitable, Callable

import httpx

from . import chain as chain_mod
from . import metrics as m
from . import probe as probe_mod
from . import sidecar
from .config import Config
from . import consul as consul_mod

log = logging.getLogger("synthmon")

Task = Callable[[], Awaitable[None]]


async def run_check(name: str, fn: Task) -> bool:
    """Run one check, never raising, and record success/duration/last-run."""
    started = time.perf_counter()
    ok = True
    try:
        await fn()
    except Exception as exc:  # noqa: BLE001 - a failing check must not kill the loop
        ok = False
        m.CHECK_FAILURES.labels(name, type(exc).__name__).inc()
        log.warning("check %s failed: %s: %s", name, type(exc).__name__, exc)
    m.CHECK_UP.labels(name).set(1 if ok else 0)
    m.CHECK_DURATION.labels(name).set(time.perf_counter() - started)
    m.CHECK_LAST_RUN.labels(name).set(time.time())
    return ok


async def periodic(name: str, fn: Task, interval: float, jitter: float) -> None:
    # Initial random delay so clusters started together do not probe in lockstep.
    await asyncio.sleep(random.uniform(0, max(jitter, 0)))
    while True:
        await run_check(name, fn)
        await asyncio.sleep(max(0.1, interval + random.uniform(-jitter, jitter)))


class Monitor:
    """Builds and owns every background loop for one cluster."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.http = probe_mod.make_client(cfg)
        self.consul = consul_mod.ConsulClient(cfg)
        self.tasks: list[asyncio.Task] = []
        self.last_probes: dict[str, dict] = {}
        self.last_chain: dict | None = None

    # --- task bodies ---------------------------------------------------------
    async def _probe_target(self, target) -> None:
        res = await probe_mod.probe_target(self.http, self.cfg, target)
        probe_mod.record(res)
        self.last_probes[f"{target.name}/{target.probe}"] = res.as_dict()

    async def _chain(self) -> None:
        self.last_chain = await chain_mod.run_chain(self.http, self.cfg)

    async def _heartbeat(self) -> None:
        url = self.cfg.heartbeat.push_url
        assert url
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(url)
                r.raise_for_status()
            m.HEARTBEAT_PUSH_UP.set(1)
        except httpx.HTTPError:
            m.HEARTBEAT_PUSH_UP.set(0)
            raise

    # --- wiring --------------------------------------------------------------
    def start(self) -> None:
        cfg, j = self.cfg, self.cfg.jitter_seconds
        cp = cfg.control_plane_interval_seconds
        m.BUILD_INFO.labels(cfg.cluster, cfg.version).set(1)

        def add(name: str, fn: Task, interval: float) -> None:
            self.tasks.append(asyncio.create_task(periodic(name, fn, interval, j), name=name))

        for t in cfg.targets:
            add(f"probe:{t.name}/{t.probe}", (lambda t=t: self._probe_target(t)), cfg.interval_seconds)
        if cfg.chain.enabled and cfg.chain.initiate:
            add("chain", self._chain, cfg.chain.interval_seconds)
        if cfg.peering.enabled:
            add("peering", lambda: consul_mod.check_peering(self.consul, cfg), cp)
        if cfg.servers.enabled:
            add("servers", lambda: consul_mod.check_servers(self.consul, cfg), cp)
            add("ca_license", lambda: consul_mod.check_ca_and_license(self.consul, cfg), cp * 4)
            add("acl", lambda: consul_mod.check_acl(self.consul, cfg), cp)
        if cfg.canary.enabled:
            add("canary_publish", lambda: consul_mod.publish_canary(self.consul, cfg), cfg.canary.interval_seconds)
            add("canary_observe", lambda: consul_mod.observe_canary(self.consul, cfg), cfg.canary.interval_seconds / 2)
        if cfg.envoy.enabled:
            add("envoy", lambda: sidecar.check_envoy(cfg), cp)
        if cfg.dns.enabled and cfg.dns.names:
            add("dns", lambda: sidecar.check_dns(cfg), cfg.interval_seconds)
        if cfg.heartbeat.push_url:
            add("heartbeat_push", self._heartbeat, cfg.heartbeat.interval_seconds)

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.http.aclose()
        await self.consul.aclose()
