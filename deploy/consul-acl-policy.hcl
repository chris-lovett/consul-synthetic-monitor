# Consul ACL policy for the synthmon token (HCL). Read-mostly; write only for the canary.
service "synthmon" { policy = "read" }
service "synthmon-canary" { policy = "write" }
service_prefix "" { policy = "read" }   # mesh gateway health lookups, peer imports
node_prefix "" { policy = "read" }
node "synthmon-canary-cluster-a" { policy = "write" }   # one per cluster: synthmon-canary-<cluster>
operator = "read"                        # autopilot health, license, raft status
mesh = "read"                            # CA roots
peering = "read"                         # list peerings
acl = "read"                             # /v1/acl/token/self works for any valid token
