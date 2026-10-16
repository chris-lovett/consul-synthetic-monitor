import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from synthmon import consul as c, metrics as m
from synthmon.config import Config


def val(name, **labels):
    return m.REGISTRY.get_sample_value(name, labels)


def iso(offset_seconds: float, nanos: bool = True) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    s = dt.strftime("%Y-%m-%dT%H:%M:%S")
    return s + (".123456789Z" if nanos else "Z")


def consul_for(routes: dict, cfg: Config) -> c.ConsulClient:
    def handler(request: httpx.Request) -> httpx.Response:
        r = routes.get(request.url.path)
        if r is None:
            return httpx.Response(404)
        if callable(r):
            return r(request)
        return httpx.Response(200, json=r)
    http = httpx.AsyncClient(base_url="http://consul", transport=httpx.MockTransport(handler))
    return c.ConsulClient(cfg, http)


def test_parse_ts_handles_go_nanoseconds_and_zero():
    assert abs(c.parse_ts(iso(0)) - time.time()) < 2
    assert abs(c.parse_ts(iso(0, nanos=False)) - time.time()) < 2
    assert c.parse_ts("2030-01-01T00:00:00.5+02:00") is not None
    assert c.parse_ts("0001-01-01T00:00:00Z") is None
    assert c.parse_ts(None) is None
    assert c.parse_ts("garbage") is None


async def test_peering_state_drift_and_heartbeat():
    cfg = Config.model_validate({
        "cluster": "a",
        "peering": {"expected": [
            {"name": "pb", "exported": ["web", "api"], "imported": ["db"]},
            {"name": "ghost"},
        ]},
    })
    peers = [
        {"Name": "pb", "State": "ACTIVE", "StreamStatus": {
            "ExportedServices": ["web"], "ImportedServices": ["db"], "LastHeartbeat": iso(-30)}},
        {"Name": "pc", "State": "FAILING", "StreamStatus": {}},
    ]
    cc = consul_for({"/v1/peerings": peers}, cfg)
    await c.check_peering(cc, cfg)

    assert val("synthmon_consul_peering_up", peer="pb") == 1
    assert val("synthmon_consul_peering_up", peer="pc") == 0
    assert val("synthmon_consul_peering_state", peer="pc", state="FAILING") == 1
    assert val("synthmon_consul_peering_state", peer="pc", state="ACTIVE") == 0
    # expected peering that does not exist at all
    assert val("synthmon_consul_peering_up", peer="ghost") == 0
    assert val("synthmon_consul_peering_state", peer="ghost", state="MISSING") == 1
    # drift: 'api' not exported, 'web' and 'db' present
    assert val("synthmon_consul_peering_expected_service_missing", peer="pb", direction="exported", service="api") == 1
    assert val("synthmon_consul_peering_expected_service_missing", peer="pb", direction="exported", service="web") == 0
    assert val("synthmon_consul_peering_expected_service_missing", peer="pb", direction="imported", service="db") == 0
    assert 25 < val("synthmon_consul_peering_heartbeat_age_seconds", peer="pb") < 40


async def test_servers_leader_autopilot_and_gateways():
    cfg = Config.model_validate({"cluster": "a", "servers": {"mesh_gateways": ["mesh-gateway"], "min_gateway_instances": 2}})
    routes = {
        "/v1/status/leader": "10.0.0.1:8300",
        "/v1/operator/autopilot/health": {
            "Healthy": False, "FailureTolerance": 0,
            "Servers": [{"Name": "s1", "Healthy": True}, {"Name": "s2", "Healthy": False}]},
        "/v1/agent/self": {"Config": {"Version": "1.18.2+ent"}},
        "/v1/health/service/mesh-gateway": [{"Service": {}}],
    }
    await c.check_servers(consul_for(routes, cfg), cfg)
    assert val("synthmon_consul_raft_leader_present") == 1
    assert val("synthmon_consul_autopilot_healthy") == 0
    assert val("synthmon_consul_server_healthy", server="s2") == 0
    assert val("synthmon_consul_mesh_gateway_healthy_instances", service="mesh-gateway") == 1
    assert val("synthmon_consul_mesh_gateway_below_minimum", service="mesh-gateway") == 1
    assert val("synthmon_consul_info", version="1.18.2+ent") == 1


async def test_no_leader_sets_zero():
    cfg = Config.model_validate({"cluster": "a", "servers": {"mesh_gateways": []}})
    routes = {"/v1/status/leader": "", "/v1/operator/autopilot/health": {"Healthy": True, "Servers": []}}
    await c.check_servers(consul_for(routes, cfg), cfg)
    assert val("synthmon_consul_raft_leader_present") == 0


async def test_ca_roots_and_license_expiry():
    cfg = Config.model_validate({"cluster": "a"})
    routes = {
        "/v1/connect/ca/roots": {"Roots": [
            {"Active": False, "NotAfter": iso(100)}, {"Active": True, "NotAfter": iso(86400 * 10)}]},
        "/v1/operator/license": {"License": {"expiration_time": iso(86400 * 3)}},
    }
    await c.check_ca_and_license(consul_for(routes, cfg), cfg)
    assert val("synthmon_consul_ca_roots") == 2  # rotation underway
    assert 86400 * 9.9 < val("synthmon_consul_ca_root_expiry_seconds") < 86400 * 10.1
    assert 86400 * 2.9 < val("synthmon_consul_license_expiry_seconds") < 86400 * 3.1


async def test_oss_server_without_license_endpoint_is_not_an_error():
    cfg = Config.model_validate({"cluster": "a"})
    routes = {"/v1/connect/ca/roots": {"Roots": []}}  # license route -> 404
    await c.check_ca_and_license(consul_for(routes, cfg), cfg)


async def test_acl_token_ok_and_rejected():
    cfg = Config.model_validate({"cluster": "a"})
    await c.check_acl(consul_for({"/v1/acl/token/self": {"ExpirationTime": iso(3600)}}, cfg), cfg)
    assert val("synthmon_consul_acl_token_ok") == 1
    assert 3500 < val("synthmon_consul_acl_token_expiry_seconds") < 3700

    def deny(_):
        return httpx.Response(403, text="ACL not found")
    await c.check_acl(consul_for({"/v1/acl/token/self": deny}, cfg), cfg)
    assert val("synthmon_consul_acl_token_ok") == 0


async def test_acl_server_error_is_raised_not_masked():
    cfg = Config.model_validate({"cluster": "a"})
    with pytest.raises(httpx.HTTPStatusError):
        await c.check_acl(consul_for({"/v1/acl/token/self": lambda _: httpx.Response(500)}, cfg), cfg)


async def test_canary_publish_and_propagation_observed():
    cfg = Config.model_validate({"cluster": "a", "canary": {"enabled": True, "peers": ["pb"]}})
    sent = {}

    def register(request):
        import json
        sent.update(json.loads(request.content))
        return httpx.Response(200)

    await c.publish_canary(consul_for({"/v1/catalog/register": register}, cfg), cfg)
    assert sent["Service"]["Meta"]["origin"] == "a" and sent["Service"]["Service"] == "synthmon-canary"
    assert val("synthmon_canary_publish_up") == 1

    c._seen_canary.clear()
    stamp = [f"{time.time() - 40:.3f}"]

    def health(_):
        return httpx.Response(200, json=[{"Service": {"Meta": {"published_at": stamp[0]}}}])

    cc = consul_for({"/v1/health/service/synthmon-canary": health}, cfg)
    await c.observe_canary(cc, cfg)  # first sighting after start: age only, no propagation sample
    assert 39 < val("synthmon_canary_age_seconds", origin="pb", observer="a") < 45
    assert val("synthmon_canary_propagation_seconds_count", origin="pb", observer="a") in (None, 0)

    stamp[0] = f"{time.time() - 3:.3f}"  # a newer canary arrives
    await c.observe_canary(cc, cfg)
    assert val("synthmon_canary_propagation_seconds_count", origin="pb", observer="a") == 1


async def test_canary_missing_is_infinite_age():
    cfg = Config.model_validate({"cluster": "z", "canary": {"enabled": True, "peers": ["pq"]}})
    cc = consul_for({"/v1/health/service/synthmon-canary": []}, cfg)
    await c.observe_canary(cc, cfg)
    assert val("synthmon_canary_age_seconds", origin="pq", observer="z") == float("inf")
