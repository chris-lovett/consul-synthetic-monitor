"""FastAPI app. Every cluster runs this same app: it is both prober and probe target."""
from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import chain as chain_mod
from . import metrics as m
from .config import Config, load_config
from .probe import parse_xfcc
from .scheduler import Monitor


def create_app(cfg: Config | None = None, start_monitor: bool = True) -> FastAPI:
    cfg = cfg or load_config(os.environ.get("SYNTHMON_CONFIG", "/etc/synthmon/config.yaml"))
    monitor = Monitor(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
        logging.getLogger("httpx").setLevel(logging.WARNING)  # one INFO line per probe is noise
        if start_monitor:
            monitor.start()
        yield
        await monitor.stop()

    app = FastAPI(title="consul-synthetic-monitor", lifespan=lifespan)
    app.state.monitor = monitor
    app.state.cfg = cfg

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict:
        return {"status": "ready", "cluster": cfg.cluster}

    @app.get("/ping")
    async def ping(request: Request) -> dict:
        """Probe target. Echoes who we are and which mTLS identity the caller presented."""
        xfcc = parse_xfcc(request.headers.get("x-forwarded-client-cert"))
        return {
            "cluster": cfg.cluster, "service": cfg.service_name, "version": cfg.version,
            "time": time.time(),
            "client_identity": {"uri": xfcc.get("uri"), "hash": xfcc.get("hash")} if xfcc else {},
        }

    @app.get("/chain")
    async def chain(
        origin: str, chain_id: str, hops_left: int,
        visited: str = "", deadline: float = Query(default=0.0),
    ) -> dict:
        """Relay a chain probe to the next cluster in the ring (see chain.py)."""
        return await chain_mod.relay(
            monitor.http, cfg, origin=origin, chain_id=chain_id,
            visited=[v for v in visited.split(",") if v],
            hops_left=min(hops_left, chain_mod.MAX_HOPS_HARD_CAP),
            deadline=deadline or time.time() + cfg.chain.timeout_seconds,
        )

    @app.get("/status")
    async def status() -> dict:
        return {"cluster": cfg.cluster, "version": cfg.version,
                "probes": monitor.last_probes, "chain": monitor.last_chain}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(m.REGISTRY), media_type=CONTENT_TYPE_LATEST)

    return app


def app_factory() -> FastAPI:  # uvicorn --factory synthmon.app:app_factory
    return create_app()
