"""Prometheus metrics. Labels follow source_cluster / dest_cluster / peer conventions."""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()

_LAT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
_PAIR = ["source_cluster", "dest_cluster", "probe"]


def _g(name: str, doc: str, labels: list[str] | None = None) -> Gauge:
    return Gauge(name, doc, labels or [], registry=REGISTRY)


# --- data plane probes -------------------------------------------------------
PROBE_UP = _g("synthmon_probe_up", "1 if the last probe met expectations", _PAIR)
PROBE_DURATION = Histogram(
    "synthmon_probe_duration_seconds", "End-to-end probe latency", _PAIR,
    buckets=_LAT_BUCKETS, registry=REGISTRY,
)
PROBE_TOTAL = Counter(
    "synthmon_probe_total", "Probes by outcome and failure type",
    _PAIR + ["result", "failure_type"], registry=REGISTRY,
)
PROBE_LAST_SUCCESS = _g(
    "synthmon_probe_last_success_timestamp_seconds", "Unix time of last successful probe", _PAIR
)
PROBE_HTTP_STATUS = _g("synthmon_probe_http_status", "Last HTTP status (0 if none)", _PAIR)
PROBE_IDENTITY_OK = _g(
    "synthmon_probe_identity_ok",
    "1 if the destination saw the expected mTLS client identity (only set when XFCC is present)",
    ["source_cluster", "dest_cluster"],
)

# --- chain / ring probes -----------------------------------------------------
CHAIN_UP = _g("synthmon_chain_up", "1 if the last full chain completed", ["origin"])
CHAIN_DURATION = Histogram(
    "synthmon_chain_duration_seconds", "Total chain latency", ["origin"],
    buckets=_LAT_BUCKETS, registry=REGISTRY,
)
CHAIN_HOPS_OK = _g("synthmon_chain_hops_ok", "Hops that succeeded in the last chain", ["origin"])
CHAIN_CLUSTERS_VISITED = _g(
    "synthmon_chain_clusters_visited", "Distinct clusters reached in the last chain", ["origin"]
)
CHAIN_HOP_UP = _g(
    "synthmon_chain_hop_up", "1 if the hop succeeded in the last chain",
    ["origin", "source_cluster", "dest_cluster"],
)
CHAIN_HOP_LATENCY = _g(
    "synthmon_chain_hop_latency_seconds", "Per-hop latency measured by the relaying cluster",
    ["origin", "source_cluster", "dest_cluster"],
)
CHAIN_TOTAL = Counter(
    "synthmon_chain_total", "Chains by outcome", ["origin", "result", "failure_type"],
    registry=REGISTRY,
)

# --- control plane -----------------------------------------------------------
PEERING_UP = _g("synthmon_consul_peering_up", "1 if the peering is ACTIVE", ["peer"])
PEERING_STATE = _g("synthmon_consul_peering_state", "Current peering state", ["peer", "state"])
PEERING_SERVICES = _g(
    "synthmon_consul_peering_services", "Services exported/imported on a peering",
    ["peer", "direction"],
)
PEERING_SERVICE_MISSING = _g(
    "synthmon_consul_peering_expected_service_missing",
    "1 if an expected exported/imported service is absent (drift)", ["peer", "direction", "service"],
)
PEERING_HEARTBEAT_AGE = _g(
    "synthmon_consul_peering_heartbeat_age_seconds", "Age of last peering stream heartbeat", ["peer"]
)
RAFT_LEADER = _g("synthmon_consul_raft_leader_present", "1 if the cluster has a Raft leader")
AUTOPILOT_HEALTHY = _g("synthmon_consul_autopilot_healthy", "1 if autopilot reports healthy")
AUTOPILOT_FAILURE_TOLERANCE = _g(
    "synthmon_consul_autopilot_failure_tolerance", "Servers that can fail without losing quorum"
)
SERVER_HEALTHY = _g("synthmon_consul_server_healthy", "Autopilot health per server", ["server"])
LICENSE_EXPIRY = _g(
    "synthmon_consul_license_expiry_seconds", "Seconds until the Consul Enterprise license expires"
)
CA_ROOT_EXPIRY = _g(
    "synthmon_consul_ca_root_expiry_seconds", "Seconds until the active CA root expires"
)
CA_ROOT_COUNT = _g("synthmon_consul_ca_roots", "Trusted CA roots (>1 means a rotation is underway)")
ACL_TOKEN_OK = _g("synthmon_consul_acl_token_ok", "1 if the probe ACL token resolves")
ACL_TOKEN_EXPIRY = _g(
    "synthmon_consul_acl_token_expiry_seconds", "Seconds until the probe ACL token expires"
)
GATEWAY_INSTANCES = _g(
    "synthmon_consul_mesh_gateway_healthy_instances", "Passing mesh gateway instances", ["service"]
)
GATEWAY_BELOW_MIN = _g(
    "synthmon_consul_mesh_gateway_below_minimum", "1 if passing instances < configured minimum",
    ["service"],
)
CONSUL_INFO = _g("synthmon_consul_info", "Consul server version", ["version"])

CANARY_AGE = _g(
    "synthmon_canary_age_seconds", "Age of the newest canary observed from a peer",
    ["origin", "observer"],
)
CANARY_PROPAGATION = Histogram(
    "synthmon_canary_propagation_seconds", "Delay between canary publish and first sighting",
    ["origin", "observer"], buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300),
    registry=REGISTRY,
)
CANARY_PUBLISH_UP = _g("synthmon_canary_publish_up", "1 if the local canary was published")

DNS_UP = _g("synthmon_dns_up", "1 if the DNS name resolved", ["name", "type"])
DNS_DURATION = _g("synthmon_dns_duration_seconds", "DNS resolution time", ["name", "type"])
DNS_ANSWERS = _g("synthmon_dns_answers", "Records returned", ["name", "type"])

# --- sidecar -----------------------------------------------------------------
ENVOY_CERT_EXPIRY = _g(
    "synthmon_envoy_cert_expiry_seconds", "Soonest expiry of certs loaded in the sidecar",
    ["kind"],
)
ENVOY_XDS_CONNECTED = _g("synthmon_envoy_xds_connected", "1 if sidecar is connected to its xDS server")
ENVOY_XDS_REJECTED = _g(
    "synthmon_envoy_xds_updates_rejected", "Cumulative xDS updates NACKed by the sidecar", ["xds"]
)
ENVOY_XDS_FAILURE = _g(
    "synthmon_envoy_xds_update_failures", "Cumulative xDS update failures", ["xds"]
)
ENVOY_XDS_AGE = _g(
    "synthmon_envoy_xds_last_update_age_seconds", "Seconds since last successful xDS update",
    ["xds"],
)
ENVOY_LIVE = _g("synthmon_envoy_live", "1 if sidecar reports state LIVE")
ENVOY_INFO = _g("synthmon_envoy_info", "Envoy version", ["version"])

# --- monitor self-observability ---------------------------------------------
CHECK_UP = _g("synthmon_check_up", "1 if the check ran without error", ["check"])
CHECK_DURATION = _g("synthmon_check_duration_seconds", "Check run time", ["check"])
CHECK_LAST_RUN = _g("synthmon_check_last_run_timestamp_seconds", "Unix time of last run", ["check"])
CHECK_FAILURES = Counter(
    "synthmon_check_failures_total", "Check execution errors", ["check", "error"], registry=REGISTRY
)
HEARTBEAT_PUSH_UP = _g("synthmon_heartbeat_push_up", "1 if the outbound heartbeat ping worked")
BUILD_INFO = _g("synthmon_build_info", "Build and identity info", ["cluster", "version"])
