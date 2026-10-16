"""Configuration models. The same image runs in every cluster; only the config differs."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class Target(BaseModel):
    """A remote instance of this application, reached through the local sidecar."""

    name: str  # destination cluster name; also the default expected responder
    url: str  # base URL, e.g. http://synthmon.virtual.cluster-b.peer.consul
    expect: Literal["success", "denied"] = "success"
    expect_clusters: list[str] = Field(default_factory=list)  # defaults to [name]
    expect_status: int = 200
    probe: str = ""  # metric label; defaults to "direct" or "negative_intention"

    @model_validator(mode="after")
    def _defaults(self) -> "Target":
        if not self.expect_clusters:
            self.expect_clusters = [self.name]
        if not self.probe:
            self.probe = "negative_intention" if self.expect == "denied" else "direct"
        return self


class ChainCfg(BaseModel):
    enabled: bool = False
    next: Target | None = None  # next hop in the ring
    initiate: bool = True  # start chains from this cluster
    interval_seconds: float = 60
    timeout_seconds: float = 15
    max_hops: int = 16
    expected_clusters: list[str] = Field(default_factory=list)  # full ring membership


class ConsulCfg(BaseModel):
    http_addr: str = "http://127.0.0.1:8500"
    token_env: str = "CONSUL_HTTP_TOKEN"
    partition: str | None = None
    namespace: str | None = None
    ca_file: str | None = None
    verify: bool = True
    timeout_seconds: float = 5


class PeerExpectation(BaseModel):
    name: str
    exported: list[str] = Field(default_factory=list)  # services we expect to export to the peer
    imported: list[str] = Field(default_factory=list)  # services we expect to import from the peer


class PeeringCfg(BaseModel):
    enabled: bool = True
    expected: list[PeerExpectation] = Field(default_factory=list)
    max_heartbeat_age_seconds: float = 120


class ServersCfg(BaseModel):
    enabled: bool = True
    mesh_gateways: list[str] = Field(default_factory=lambda: ["mesh-gateway"])
    min_gateway_instances: int = 1


class CanaryCfg(BaseModel):
    """Control-plane propagation canary. Needs the canary service exported to every peer."""

    enabled: bool = False
    service_name: str = "synthmon-canary"
    interval_seconds: float = 30
    peers: list[str] = Field(default_factory=list)  # peers whose canary we observe


class DnsName(BaseModel):
    name: str
    type: str = "A"


class DnsCfg(BaseModel):
    enabled: bool = False
    server: str | None = None  # host:port of Consul DNS; None uses the system resolver
    names: list[DnsName] = Field(default_factory=list)
    timeout_seconds: float = 3


class EnvoyCfg(BaseModel):
    enabled: bool = True
    admin_addr: str = "http://127.0.0.1:19000"
    timeout_seconds: float = 3


class HeartbeatCfg(BaseModel):
    """Optional outbound dead-man's-switch ping (healthchecks.io, Alertmanager Watchdog bridge)."""

    push_url: str | None = None
    interval_seconds: float = 60


class Config(BaseModel):
    cluster: str
    service_name: str = "synthmon"
    version: str = "dev"
    interval_seconds: float = 15
    jitter_seconds: float = 5
    timeout_seconds: float = 5
    control_plane_interval_seconds: float = 30
    targets: list[Target] = Field(default_factory=list)
    chain: ChainCfg = Field(default_factory=ChainCfg)
    consul: ConsulCfg = Field(default_factory=ConsulCfg)
    peering: PeeringCfg = Field(default_factory=PeeringCfg)
    servers: ServersCfg = Field(default_factory=ServersCfg)
    canary: CanaryCfg = Field(default_factory=CanaryCfg)
    dns: DnsCfg = Field(default_factory=DnsCfg)
    envoy: EnvoyCfg = Field(default_factory=EnvoyCfg)
    heartbeat: HeartbeatCfg = Field(default_factory=HeartbeatCfg)

    @property
    def consul_token(self) -> str | None:
        return os.environ.get(self.consul.token_env) or None


def load_config(path: str | os.PathLike[str]) -> Config:
    data = yaml.safe_load(Path(path).read_text()) or {}
    # The image is identical everywhere, so identity can be injected by the environment.
    if os.environ.get("SYNTHMON_CLUSTER"):
        data["cluster"] = os.environ["SYNTHMON_CLUSTER"]
    if os.environ.get("SYNTHMON_VERSION"):
        data["version"] = os.environ["SYNTHMON_VERSION"]
    return Config.model_validate(data)
