import json
import time
from datetime import datetime, timedelta, timezone

import httpx
from fastapi.testclient import TestClient

from synthmon import metrics as m, sidecar
from synthmon.app import create_app
from synthmon.config import Config, load_config
from synthmon.scheduler import run_check


def val(metric, **labels):
    return m.REGISTRY.get_sample_value(metric, labels)


def iso(offset):
    return (datetime.now(timezone.utc) + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")


STATS = """\
cluster_manager.cds.update_rejected: 2
cluster_manager.cds.update_success: 10
cluster_manager.cds.update_failure: 1
listener_manager.lds.update_rejected: 0
listener_manager.lds.update_success: 5
control_plane.connected_state: 1
cluster.local_app.update_success: 99
"""


def test_parse_xds_stats():
    s = sidecar.parse_xds_stats(STATS)
    assert s["rejected"] == {"cds": 2, "lds": 0}
    assert s["failure"]["cds"] == 1
    assert s["success"]["cds"] == 10
    assert s["connected"]["all"] == 1


async def test_check_envoy_reports_cert_expiry_xds_and_liveness():
    cfg = Config.model_validate({"cluster": "a"})
    certs = {"certificates": [{
        "cert_chain": [{"expiration_time": iso(7200)}, {"expiration_time": iso(3600)}],
        "ca_certs": [{"expiration_time": iso(86400)}],
    }]}

    def handler(request):
        p = request.url.path
        if p == "/ready":
            return httpx.Response(200, text="LIVE\n")
        if p == "/server_info":
            return httpx.Response(200, json={"version": "abc/1.29.4/Clean/RELEASE"})
        if p == "/certs":
            return httpx.Response(200, json=certs)
        if p == "/stats":
            return httpx.Response(200, text=STATS)
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as cl:
        await sidecar.check_envoy(cfg, cl)

    assert val("synthmon_envoy_live") == 1
    assert 3500 < val("synthmon_envoy_cert_expiry_seconds", kind="leaf") < 3700  # soonest wins
    assert 86000 < val("synthmon_envoy_cert_expiry_seconds", kind="ca") < 86500
    assert val("synthmon_envoy_xds_updates_rejected", xds="cds") == 2
    assert val("synthmon_envoy_xds_connected") == 1
    assert val("synthmon_envoy_info", version="1.29.4") == 1


async def test_run_check_swallows_errors_and_records_them():
    async def boom():
        raise RuntimeError("nope")

    assert await run_check("boomcheck", boom) is False
    assert val("synthmon_check_up", check="boomcheck") == 0
    assert val("synthmon_check_failures_total", check="boomcheck", error="RuntimeError") == 1

    async def fine():
        pass

    assert await run_check("finecheck", fine) is True
    assert val("synthmon_check_up", check="finecheck") == 1
    assert val("synthmon_check_last_run_timestamp_seconds", check="finecheck") > time.time() - 5


def test_http_endpoints():
    cfg = Config.model_validate({"cluster": "edge-1", "version": "9.9"})
    with TestClient(create_app(cfg, start_monitor=False)) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/readyz").status_code == 200

        body = client.get("/ping").json()
        assert body["cluster"] == "edge-1" and body["client_identity"] == {}

        xfcc = "By=spiffe://x;Hash=h1;URI=spiffe://td/ns/default/dc/dc1/svc/synthmon"
        body = client.get("/ping", headers={"x-forwarded-client-cert": xfcc}).json()
        assert body["client_identity"]["uri"].endswith("/svc/synthmon")

        metrics = client.get("/metrics")
        assert metrics.status_code == 200 and "synthmon_" in metrics.text
        assert client.get("/status").json()["cluster"] == "edge-1"


def test_chain_endpoint_caps_hops_and_has_no_next_hop():
    cfg = Config.model_validate({"cluster": "end"})  # no next hop configured
    with TestClient(create_app(cfg, start_monitor=False)) as client:
        r = client.get("/chain", params={"origin": "o", "chain_id": "c", "hops_left": 9999, "visited": "o"})
        assert r.status_code == 200
        assert r.json()["visited"] == ["o", "end"] and r.json()["ok"]


def test_load_config_env_overrides(tmp_path, monkeypatch):
    f = tmp_path / "c.yaml"
    f.write_text("cluster: from-file\ntargets:\n  - {name: b, url: 'http://b'}\n")
    monkeypatch.setenv("SYNTHMON_CLUSTER", "from-env")
    cfg = load_config(f)
    assert cfg.cluster == "from-env"
    assert cfg.targets[0].expect_clusters == ["b"]  # defaults to the target name


async def test_dns_check_failure_sets_down(monkeypatch):
    cfg = Config.model_validate({
        "cluster": "a",
        "dns": {"enabled": True, "server": "127.0.0.1:1", "timeout_seconds": 0.3,
                "names": [{"name": "nothing.service.consul"}]},
    })
    await sidecar.check_dns(cfg)
    assert val("synthmon_dns_up", name="nothing.service.consul", type="A") == 0
    assert val("synthmon_dns_duration_seconds", name="nothing.service.consul", type="A") >= 0
