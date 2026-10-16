# Consul Synthetic Monitor (synthmon)

A heartbeat service deployed identically in every Consul cluster. Each instance is both a
**prober** (calls its peers through the mesh) and a **target** (answers `/ping` and `/chain`).
It exports Prometheus metrics and ships alert rules so failures are attributed to a specific
`source_cluster -> dest_cluster` pair, not just "something broke".

## What it checks

| Area | Check | Metrics (prefix `synthmon_`) |
|---|---|---|
| Data plane | Direct pair probes through app → sidecar → mesh gateway → remote sidecar → app. Latency histogram, status, last success. | `probe_up`, `probe_duration_seconds`, `probe_total{result,failure_type}` |
| Failure triage | Failures classified as `dns`, `connect`, `tls`, `timeout`, `intention_denied`, `http_5xx`, `http_status`, `wrong_responder`, `bad_body` | `probe_total{failure_type}` |
| mTLS identity | Responder echoes the SPIFFE URI from `x-forwarded-client-cert`; caller verifies it is `.../svc/<service_name>` | `probe_identity_ok` |
| Negative intentions | Target with `expect: denied` must be refused. A successful call fails the probe (`unexpected_allow`). | `probe_up{probe="negative_intention"}` |
| Ring / chain | A → B → C → A with a hop cap, loop detection, deadline and ring-membership check. Every hop is reported separately. | `chain_up`, `chain_hop_up`, `chain_hop_latency_seconds`, `chain_clusters_visited` |
| Peering | State (`ACTIVE` etc.), heartbeat age, expected exported/imported services (drift) | `consul_peering_up`, `consul_peering_state`, `consul_peering_heartbeat_age_seconds`, `consul_peering_expected_service_missing` |
| Propagation canary | Each cluster re-registers a timestamped canary service; peers measure age and propagation delay | `canary_age_seconds`, `canary_propagation_seconds` |
| Servers | Raft leader, autopilot health and tolerance, server version, mesh gateway passing instances | `consul_raft_leader_present`, `consul_autopilot_healthy`, `consul_mesh_gateway_below_minimum` |
| Certificates | Active CA root expiry, number of roots (rotation), sidecar leaf and CA cert expiry, Enterprise license expiry | `consul_ca_root_expiry_seconds`, `envoy_cert_expiry_seconds`, `consul_license_expiry_seconds` |
| xDS | Sidecar connected to control plane, rejected and failed updates, time since last update | `envoy_xds_connected`, `envoy_xds_updates_rejected`, `envoy_xds_last_update_age_seconds` |
| ACL | Probe token still resolves; token expiry | `consul_acl_token_ok`, `consul_acl_token_expiry_seconds` |
| DNS | Resolution success, time and answer count for configured Consul DNS names | `dns_up`, `dns_duration_seconds` |
| The monitor itself | Per-check success, duration, last run; optional outbound dead-man's-switch ping | `check_up`, `check_last_run_timestamp_seconds`, `heartbeat_push_up` |

## Design notes

- **Pair probes are the primary signal; the chain is secondary.** A chain alone hides downstream
  failures. Here every relay returns its own measured hop plus the nested downstream hops, so a
  break at B → C is reported as `chain_hop_up{source_cluster="B",dest_cluster="C"} == 0` and the
  hops before and after it remain visible.
- **Loop safety.** `max_hops` (hard cap 64), a visited list that detects loops, and an absolute
  deadline carried in each request. A loop that never returns to the origin is `loop_detected`;
  a ring missing a configured member is `ring_incomplete`.
- **Peering is not transitive.** B calls C through B's own peering and local upstream, so the
  chain tests exactly the paths you configure.
- **A dead cluster cannot report itself.** Alert on missing data (`SynthmonMissing`) and use the
  peer-side view (`SynthmonClusterUnreachableFromPeers`). Optionally set `heartbeat.push_url`
  to an external dead-man's-switch service, and run at least one blackbox probe from outside the mesh.
- **Jitter** on every loop prevents clusters from probing in lockstep.
- **Per-pod metrics.** Run one replica per cluster, or aggregate by `source_cluster` if you run more.

## Layout

```
synthmon/            application (config, probe, chain, consul, sidecar, scheduler, app)
tests/               pytest suite (in-process mesh simulation + mocked Consul/Envoy APIs)
config.example.yaml  annotated configuration
deploy/kubernetes/   Deployment, ConfigMap, Service, ServiceDefaults, ServiceIntentions, ExportedServices
deploy/consul-acl-policy.hcl
deploy/prometheus-rules.yaml
Dockerfile
```

## Run locally

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
SYNTHMON_CONFIG=config.example.yaml .venv/bin/uvicorn synthmon.app:app_factory --factory --port 8080
curl localhost:8080/ping ; curl localhost:8080/status ; curl localhost:8080/metrics
```

## Deploying to a cluster

1. Build and push the image (`docker build -t REGISTRY/synthmon:0.1.0 .`).
2. Create the Consul ACL policy from `deploy/consul-acl-policy.hcl` (adjust the canary node name
   per cluster) and store its token in a secret named `synthmon-consul-token`.
3. In each cluster, edit the ConfigMap in `deploy/kubernetes/synthmon.yaml`: `SYNTHMON_CLUSTER`,
   `targets`, `chain.next`, `peering.expected`, `canary.peers`, and the upstream annotation.
4. Apply it, then load `deploy/prometheus-rules.yaml` into your central Prometheus.

## Things to verify in your environment

This was built from knowledge of Consul's APIs and has not been run against a live Consul mesh.
Check these against your versions:

- **Peering API response shape.** `check_peering` reads `StreamStatus.ExportedServices`,
  `ImportedServices` and `LastHeartbeat` from `GET /v1/peerings`.
- **Upstream annotation / virtual peer DNS names** for your consul-k8s version.
- **Envoy admin access.** `envoy.admin_addr` assumes the admin port is reachable at
  `127.0.0.1:19000` from the app container. With consul-dataplane this may need to be enabled.
- **Canary visibility.** `observe_canary` reads `GET /v1/health/service/<canary>?peer=<peer>`.
  Registration uses the catalog API as an external node so it works without a client agent.
- **mTLS identity echo** only appears when the destination's Envoy forwards
  `x-forwarded-client-cert` (L7 `http` protocol). Without it the identity check is skipped silently.
- **Negative intention probes** assume an L4 deny closes the connection or returns 403; both are
  treated as "denied". Other connection errors are not.
