"""Data-plane probe: HTTP calls through the mesh with failure classification."""
from __future__ import annotations

import socket
import ssl
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from . import metrics as m
from .config import Config, Target

# Failure types (kept low-cardinality; used as a metric label).
OK = "none"
DNS = "dns"
CONNECT = "connect"
TLS = "tls"
TIMEOUT = "timeout"
DENIED = "intention_denied"
HTTP_5XX = "http_5xx"
HTTP_STATUS = "http_status"
WRONG_RESPONDER = "wrong_responder"
BAD_BODY = "bad_body"
UNEXPECTED_ALLOW = "unexpected_allow"
UNKNOWN = "unknown"


@dataclass
class ProbeResult:
    source: str
    dest: str
    probe: str
    ok: bool
    failure_type: str = OK
    duration: float = 0.0
    status: int = 0
    detail: str = ""
    body: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source, "dest": self.dest, "probe": self.probe, "ok": self.ok,
            "failure_type": self.failure_type, "duration_seconds": round(self.duration, 4),
            "status": self.status, "detail": self.detail,
        }


def classify_exception(exc: BaseException) -> str:
    """Map an httpx/socket exception to a failure type."""
    cause: BaseException | None = exc
    while cause is not None:
        if isinstance(cause, socket.gaierror):
            return DNS
        if isinstance(cause, ssl.SSLError):
            return TLS
        cause = cause.__cause__ or cause.__context__
    if isinstance(exc, httpx.TimeoutException):
        return TIMEOUT
    msg = str(exc).lower()
    if "name or service not known" in msg or "nodename nor servname" in msg or "temporary failure in name" in msg:
        return DNS
    if "ssl" in msg or "tls" in msg or "certificate" in msg:
        return TLS
    # An L4 intention deny makes Envoy close the connection right after accept.
    if isinstance(exc, (httpx.RemoteProtocolError, httpx.ReadError)):
        return DENIED
    if isinstance(exc, httpx.ConnectError):
        return CONNECT
    if isinstance(exc, httpx.TransportError):
        return CONNECT
    return UNKNOWN


def classify_response(resp: httpx.Response) -> str:
    if resp.status_code == 403 and ("rbac" in resp.text.lower() or "denied" in resp.text.lower()):
        return DENIED
    if resp.status_code == 403:
        return DENIED
    if resp.status_code >= 500:
        return HTTP_5XX
    return HTTP_STATUS


def record(result: ProbeResult) -> None:
    labels = (result.source, result.dest, result.probe)
    m.PROBE_UP.labels(*labels).set(1 if result.ok else 0)
    m.PROBE_DURATION.labels(*labels).observe(result.duration)
    m.PROBE_HTTP_STATUS.labels(*labels).set(result.status)
    m.PROBE_TOTAL.labels(*labels, "success" if result.ok else "failure", result.failure_type).inc()
    if result.ok:
        m.PROBE_LAST_SUCCESS.labels(*labels).set(time.time())


def make_client(cfg: Config) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=cfg.timeout_seconds,
        headers={"user-agent": f"synthmon/{cfg.version} ({cfg.cluster})"},
        follow_redirects=False,
    )


async def probe_target(
    client: httpx.AsyncClient, cfg: Config, target: Target, path: str = "/ping",
    timeout: float | None = None,
) -> ProbeResult:
    """Call one remote instance and check it against the target's expectation."""
    url = target.url.rstrip("/") + path
    started = time.perf_counter()
    res = ProbeResult(cfg.cluster, target.name, target.probe, ok=False)
    try:
        resp = await client.get(url, timeout=timeout or cfg.timeout_seconds)
        res.duration = time.perf_counter() - started
        res.status = resp.status_code
    except Exception as exc:  # noqa: BLE001 - classified below
        res.duration = time.perf_counter() - started
        res.failure_type = classify_exception(exc)
        res.detail = f"{type(exc).__name__}: {exc}"[:200]
        if target.expect == "denied" and res.failure_type == DENIED:
            res.ok, res.failure_type, res.detail = True, OK, "denied as expected"
        return res

    if target.expect == "denied":
        if resp.status_code == 403:
            res.ok, res.detail = True, "denied as expected"
        elif resp.status_code < 400:
            res.failure_type, res.detail = UNEXPECTED_ALLOW, "call succeeded but intention should deny it"
        else:
            res.failure_type, res.detail = classify_response(resp), f"status {resp.status_code}"
        return res

    if resp.status_code != target.expect_status:
        res.failure_type = classify_response(resp)
        res.detail = f"status {resp.status_code}: {resp.text[:120]}"
        return res

    try:
        res.body = resp.json()
    except ValueError:
        res.failure_type, res.detail = BAD_BODY, "response was not JSON"
        return res

    responder = res.body.get("cluster")
    if responder not in target.expect_clusters:
        res.failure_type = WRONG_RESPONDER
        res.detail = f"expected {target.expect_clusters}, got {responder!r}"
        return res

    res.ok = True
    _check_identity(cfg, target, res)
    return res


def parse_xfcc(header: str | None) -> dict[str, str]:
    """Parse the first element of Envoy's x-forwarded-client-cert header."""
    out: dict[str, str] = {}
    if not header:
        return out
    first = header.split(",")[0]
    for part in first.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip().lower()] = v.strip().strip('"')
    return out


def spiffe_trust_domain(uri: str) -> str | None:
    if uri.startswith("spiffe://"):
        return uri[len("spiffe://"):].split("/", 1)[0]
    return None


def _check_identity(cfg: Config, target: Target, res: ProbeResult) -> None:
    """The responder echoes the client identity it saw; verify it is us."""
    seen = res.body.get("client_identity") or {}
    uri = seen.get("uri")
    if not uri:
        return  # no XFCC configured on the destination; nothing to verify
    expect_svc = f"/svc/{cfg.service_name}"
    ok = uri.endswith(expect_svc)
    m.PROBE_IDENTITY_OK.labels(cfg.cluster, target.name).set(1 if ok else 0)
    if not ok:
        res.ok = False
        res.failure_type = TLS
        res.detail = f"unexpected client identity {uri}"
