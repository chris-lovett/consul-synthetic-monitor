import httpx
import pytest

from synthmon import probe
from synthmon.config import Config, Target


def cfg(**kw) -> Config:
    return Config.model_validate({"cluster": "a", **kw})


def client_for(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def ok_handler(cluster="b", identity=None):
    def h(request):
        body = {"cluster": cluster}
        if identity:
            body["client_identity"] = {"uri": identity}
        return httpx.Response(200, json=body)
    return h


async def run(handler, target=None, c=None):
    c = c or cfg()
    target = target or Target(name="b", url="http://b")
    async with client_for(handler) as cl:
        return await probe.probe_target(cl, c, target)


async def test_success():
    r = await run(ok_handler())
    assert r.ok and r.failure_type == "none" and r.status == 200


async def test_wrong_responder_is_failure():
    r = await run(ok_handler("c"))
    assert not r.ok and r.failure_type == "wrong_responder"


async def test_expect_clusters_allows_failover():
    t = Target(name="b", url="http://b", expect_clusters=["b", "b-dr"])
    r = await run(ok_handler("b-dr"), t)
    assert r.ok


@pytest.mark.parametrize(
    "exc,expected",
    [
        (httpx.ConnectError("refused"), "connect"),
        (httpx.ConnectTimeout("t"), "timeout"),
        (httpx.ReadTimeout("t"), "timeout"),
        (httpx.RemoteProtocolError("closed"), "intention_denied"),
        (httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"), "tls"),
        (httpx.ConnectError("[Errno 8] nodename nor servname provided, or not known"), "dns"),
    ],
)
async def test_exception_classification(exc, expected):
    def h(request):
        raise exc
    r = await run(h)
    assert not r.ok and r.failure_type == expected


async def test_http_5xx_and_status():
    r = await run(lambda req: httpx.Response(503, text="x"))
    assert r.failure_type == "http_5xx"
    r = await run(lambda req: httpx.Response(404, text="x"))
    assert r.failure_type == "http_status"


async def test_bad_body():
    r = await run(lambda req: httpx.Response(200, text="<html>"))
    assert r.failure_type == "bad_body"


async def test_negative_intention_denied_via_403_passes():
    t = Target(name="b", url="http://b", expect="denied")
    r = await run(lambda req: httpx.Response(403, text="RBAC: access denied"), t)
    assert r.ok


async def test_negative_intention_denied_via_reset_passes():
    t = Target(name="b", url="http://b", expect="denied")

    def h(request):
        raise httpx.RemoteProtocolError("closed")
    r = await run(h, t)
    assert r.ok and r.failure_type == "none"


async def test_negative_intention_unexpected_allow_fails():
    t = Target(name="b", url="http://b", expect="denied")
    r = await run(ok_handler(), t)
    assert not r.ok and r.failure_type == "unexpected_allow"


async def test_negative_intention_connect_error_still_fails():
    t = Target(name="b", url="http://b", expect="denied")

    def h(request):
        raise httpx.ConnectError("refused")
    r = await run(h, t)
    assert not r.ok and r.failure_type == "connect"


async def test_identity_verified_when_present():
    r = await run(ok_handler(identity="spiffe://td.consul/ns/default/dc/dc1/svc/synthmon"))
    assert r.ok


async def test_identity_mismatch_fails():
    r = await run(ok_handler(identity="spiffe://td.consul/ns/default/dc/dc1/svc/intruder"))
    assert not r.ok and r.failure_type == "tls"


def test_parse_xfcc():
    h = 'By=spiffe://a/x;Hash=abc123;URI=spiffe://td.consul/ns/default/dc/dc1/svc/synthmon'
    p = probe.parse_xfcc(h)
    assert p["hash"] == "abc123" and p["uri"].endswith("/svc/synthmon")
    assert probe.parse_xfcc(None) == {}
    assert probe.spiffe_trust_domain("spiffe://td.consul/ns/x") == "td.consul"


async def test_record_sets_metrics():
    from synthmon import metrics as m
    r = await run(ok_handler())
    probe.record(r)
    v = m.REGISTRY.get_sample_value(
        "synthmon_probe_up", {"source_cluster": "a", "dest_cluster": "b", "probe": "direct"})
    assert v == 1.0
