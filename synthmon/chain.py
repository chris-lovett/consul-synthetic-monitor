"""Chain (ring) probing with loop protection and per-hop reporting.

A request enters at an origin cluster, which asks the next cluster to relay it onward.
Every relay returns its own measured hop plus the nested downstream result, so a break
anywhere in the ring is attributable to a specific source->dest pair.
"""
from __future__ import annotations

import time
import uuid
from typing import Any

import httpx

from . import metrics as m
from .config import Config
from .probe import classify_exception

MAX_HOPS_HARD_CAP = 64


def new_chain_id() -> str:
    return uuid.uuid4().hex[:12]


async def relay(
    client: httpx.AsyncClient, cfg: Config, *, origin: str, chain_id: str,
    visited: list[str], hops_left: int, deadline: float,
) -> dict[str, Any]:
    """Handle one hop: record ourselves, then forward to the next cluster in the ring.

    Returns ``{"cluster", "visited", "hops": [...], "ok", "failure_type"}`` where ``hops``
    is the flat list of per-hop results gathered from this node downstream.
    """
    me = cfg.cluster
    visited = [*visited, me]
    base: dict[str, Any] = {"cluster": me, "chain_id": chain_id, "origin": origin, "visited": visited}

    # We have been here before. Only returning to the origin is a healthy, complete ring;
    # any other repeat is a routing loop that never reaches the origin.
    if visited.count(me) > 1:
        if me == origin:
            return {**base, "visited": visited[:-1], "hops": [], "ok": True, "failure_type": "none", "complete": True}
        return {**base, "visited": visited[:-1], "hops": [], "ok": False, "failure_type": "loop_detected"}
    if hops_left <= 0:
        return {**base, "hops": [], "ok": False, "failure_type": "max_hops_exceeded"}
    nxt = cfg.chain.next
    if nxt is None:
        return {**base, "hops": [], "ok": True, "failure_type": "none", "complete": False}

    remaining = deadline - time.time()
    if remaining <= 0:
        return {**base, "hops": [], "ok": False, "failure_type": "deadline_exceeded"}

    hop: dict[str, Any] = {"source": me, "dest": nxt.name}
    started = time.perf_counter()
    try:
        resp = await client.get(
            nxt.url.rstrip("/") + "/chain",
            params={
                "origin": origin, "chain_id": chain_id, "visited": ",".join(visited),
                "hops_left": hops_left - 1, "deadline": f"{deadline:.3f}",
            },
            timeout=max(0.5, min(remaining, cfg.chain.timeout_seconds)),
        )
        hop["latency_seconds"] = round(time.perf_counter() - started, 4)
        if resp.status_code != 200:
            hop.update(ok=False, failure_type="http_5xx" if resp.status_code >= 500 else "intention_denied"
                       if resp.status_code == 403 else "http_status", status=resp.status_code)
            return {**base, "hops": [hop], "ok": False, "failure_type": hop["failure_type"]}
        down = resp.json()
    except Exception as exc:  # noqa: BLE001
        hop.update(latency_seconds=round(time.perf_counter() - started, 4), ok=False,
                   failure_type=classify_exception(exc))
        return {**base, "hops": [hop], "ok": False, "failure_type": hop["failure_type"]}

    hop.update(ok=True, failure_type="none", status=200)
    # The downstream result is only the truth for hops *it* made; trust its own hop list.
    hops = [hop, *down.get("hops", [])]
    ok = bool(down.get("ok"))
    return {
        **base, "visited": down.get("visited", visited), "hops": hops, "ok": ok,
        "failure_type": down.get("failure_type", "none"), "complete": down.get("complete", False),
    }


_hop_series: dict[str, set[tuple[str, str]]] = {}  # origin -> (source, dest) pairs currently exported


def _drop_stale_hops(origin: str, current: set[tuple[str, str]]) -> None:
    """Hops past a failure are not attempted; remove them rather than leave a stale 'up'."""
    for src, dst in _hop_series.get(origin, set()) - current:
        for gauge in (m.CHAIN_HOP_UP, m.CHAIN_HOP_LATENCY):
            try:
                gauge.remove(origin, src, dst)
            except KeyError:
                pass
    _hop_series[origin] = current


def record_chain(cfg: Config, result: dict[str, Any], duration: float) -> None:
    origin = cfg.cluster
    hops = result.get("hops", [])
    ok_hops = [h for h in hops if h.get("ok")]
    visited = set(result.get("visited", []))
    expected = set(cfg.chain.expected_clusters)
    complete = bool(result.get("complete")) and (not expected or expected <= visited)
    success = bool(result.get("ok")) and complete

    failure_type = result.get("failure_type", "none")
    if result.get("ok") and not complete:
        failure_type = "ring_incomplete"

    m.CHAIN_UP.labels(origin).set(1 if success else 0)
    m.CHAIN_DURATION.labels(origin).observe(duration)
    m.CHAIN_HOPS_OK.labels(origin).set(len(ok_hops))
    m.CHAIN_CLUSTERS_VISITED.labels(origin).set(len(visited))
    m.CHAIN_TOTAL.labels(origin, "success" if success else "failure", "none" if success else failure_type).inc()
    _drop_stale_hops(origin, {(h["source"], h["dest"]) for h in hops})
    for h in hops:
        labels = (origin, h["source"], h["dest"])
        m.CHAIN_HOP_UP.labels(*labels).set(1 if h.get("ok") else 0)
        if "latency_seconds" in h:
            m.CHAIN_HOP_LATENCY.labels(*labels).set(h["latency_seconds"])


async def run_chain(client: httpx.AsyncClient, cfg: Config) -> dict[str, Any]:
    """Originate a chain from this cluster and record the outcome."""
    if cfg.chain.next is None:
        return {"ok": False, "failure_type": "no_next_hop", "hops": []}
    max_hops = min(cfg.chain.max_hops, MAX_HOPS_HARD_CAP)
    started = time.perf_counter()
    result = await relay(
        client, cfg, origin=cfg.cluster, chain_id=new_chain_id(), visited=[],
        hops_left=max_hops, deadline=time.time() + cfg.chain.timeout_seconds,
    )
    record_chain(cfg, result, time.perf_counter() - started)
    return result
