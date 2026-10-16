import pytest

from synthmon import chain, metrics as m


def val(name, **labels):
    return m.REGISTRY.get_sample_value(name, labels)


async def run_from(mesh, origin):
    mon = mesh.apps[origin].state.monitor
    return await chain.run_chain(mon.http, mon.cfg)


async def test_full_ring_succeeds(mesh_factory):
    mesh = mesh_factory(["ra", "rb", "rc", "rd"])
    res = await run_from(mesh, "ra")
    assert res["ok"] and res["complete"]
    assert res["visited"] == ["ra", "rb", "rc", "rd"]
    assert [(h["source"], h["dest"]) for h in res["hops"]] == [
        ("ra", "rb"), ("rb", "rc"), ("rc", "rd"), ("rd", "ra")]
    assert val("synthmon_chain_up", origin="ra") == 1
    assert val("synthmon_chain_clusters_visited", origin="ra") == 4
    assert val("synthmon_chain_hop_up", origin="ra", source_cluster="rb", dest_cluster="rc") == 1


@pytest.mark.parametrize("mode,ftype", [
    ("connect", "connect"), ("timeout", "timeout"), ("reset", "intention_denied"),
    ("500", "http_5xx"),
])
async def test_mid_chain_break_is_attributed_to_the_right_hop(mesh_factory, mode, ftype):
    mesh = mesh_factory(["pa", "pb", "pc", "pd"], broken={"pc": mode})
    res = await run_from(mesh, "pa")
    assert not res["ok"]
    failed = [h for h in res["hops"] if not h["ok"]]
    # pb -> pc is the failing hop; hops before it are still reported good.
    assert [(h["source"], h["dest"]) for h in failed] == [("pb", "pc")]
    assert failed[0]["failure_type"] == ftype
    assert val("synthmon_chain_up", origin="pa") == 0
    assert val("synthmon_chain_hop_up", origin="pa", source_cluster="pa", dest_cluster="pb") == 1
    assert val("synthmon_chain_hop_up", origin="pa", source_cluster="pb", dest_cluster="pc") == 0
    assert val("synthmon_chain_hops_ok", origin="pa") == 1


async def test_unreached_hops_are_not_left_reporting_healthy(mesh_factory):
    mesh = mesh_factory(["sa", "sb", "sc", "sd"])
    await run_from(mesh, "sa")
    assert val("synthmon_chain_hop_up", origin="sa", source_cluster="sc", dest_cluster="sd") == 1

    mesh.broken["sc"] = "connect"  # sb -> sc now fails; sc -> sd and sd -> sa are never attempted
    await run_from(mesh, "sa")
    assert val("synthmon_chain_hop_up", origin="sa", source_cluster="sb", dest_cluster="sc") == 0
    assert val("synthmon_chain_hop_up", origin="sa", source_cluster="sc", dest_cluster="sd") is None
    assert val("synthmon_chain_hop_up", origin="sa", source_cluster="sd", dest_cluster="sa") is None


async def test_first_hop_failure(mesh_factory):
    mesh = mesh_factory(["qa", "qb", "qc"], broken={"qb": "connect"})
    res = await run_from(mesh, "qa")
    assert not res["ok"] and res["hops"][0]["dest"] == "qb" and not res["hops"][0]["ok"]


async def test_hop_cap_stops_runaway_chain(mesh_factory):
    mesh = mesh_factory(["xa", "xb", "xc", "xd", "xe"])
    mon = mesh.apps["xa"].state.monitor
    mon.cfg.chain.max_hops = 2
    res = await chain.run_chain(mon.http, mon.cfg)
    assert not res["ok"] and res["failure_type"] == "max_hops_exceeded"
    assert len(mesh.calls) <= 3  # bounded, did not loop


async def test_self_loop_misconfig_terminates(mesh_factory):
    # xb points at itself: the loop guard must stop it rather than recurse until the cap.
    mesh = mesh_factory(["la", "lb"])
    mesh.apps["lb"].state.cfg.chain.next.name = "lb"
    mesh.apps["lb"].state.cfg.chain.next.url = "http://lb"
    res = await run_from(mesh, "la")
    assert len(mesh.calls) < 6
    # The ring never returned to the origin, so it must not count as healthy.
    assert not res["ok"] and res["failure_type"] == "loop_detected"
    assert val("synthmon_chain_up", origin="la") == 0


async def test_incomplete_ring_membership_is_flagged(mesh_factory):
    mesh = mesh_factory(["ia", "ib", "ic"])
    mon = mesh.apps["ia"].state.monitor
    mon.cfg.chain.expected_clusters = ["ia", "ib", "ic", "id"]  # id silently dropped from ring
    res = await chain.run_chain(mon.http, mon.cfg)
    assert res["ok"]
    assert val("synthmon_chain_up", origin="ia") == 0
    assert val("synthmon_chain_total", origin="ia", result="failure", failure_type="ring_incomplete") == 1


async def test_deadline_exceeded(mesh_factory):
    mesh = mesh_factory(["da", "db"])
    mon = mesh.apps["da"].state.monitor
    res = await chain.relay(mon.http, mon.cfg, origin="da", chain_id="x", visited=[],
                            hops_left=5, deadline=0.0)
    assert not res["ok"] and res["failure_type"] == "deadline_exceeded"
