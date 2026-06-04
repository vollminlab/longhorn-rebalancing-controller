# longhorn-rebalancing-controller

A Kubernetes controller that rebalances [Longhorn](https://longhorn.io) replica placement across worker nodes based on scheduled storage bytes — not just replica count.

Longhorn's built-in `replica-auto-balance: best-effort` only counts replicas per node, not bytes. This causes nodes with many large volumes to become significantly more loaded than nodes with many small volumes. This controller fills that gap by evicting over-placed replicas so Longhorn rebuilds them on less-loaded nodes.

## How it works

The controller watches `nodes.longhorn.io` and `replicas.longhorn.io` in `longhorn-system`. Every 5 minutes (and on any node or replica change) it computes scheduled storage bytes per node and decides whether to evict a replica.

Eviction is non-destructive: setting `spec.evictionRequested: true` on a replica tells Longhorn to schedule a replacement elsewhere before removing the original. The volume stays healthy throughout.

### Two-mode operation

**`rebalance` mode** — recovers from an imbalanced state:
- Triggers when any node exceeds the absolute `nodeUsageThreshold` (default 75%)
- Only evicts during the configured `maintenanceWindow` (default `02:00-05:00`)
- Conservative: max 2 evictions/day, 30-minute cooldown between evictions
- Graduates to `steady-state` after 3 consecutive check cycles with all nodes below threshold

**`steady-state` mode** — prevents drift once balanced:
- Triggers when the most-loaded node has >1.5× the scheduled bytes of the least-loaded node
- No maintenance window restriction — reacts promptly to small imbalances
- Max 5 evictions/day, 10-minute cooldown
- Reverts to `rebalance` if any node crosses the absolute threshold again

### Safety gates

All of the following must hold before any eviction is attempted:

1. All volumes cluster-wide have `robustness == healthy`
2. No replica is currently in `currentState == rebuilding`
3. Cooldown period elapsed since last eviction
4. Daily eviction count below cap
5. Within maintenance window (rebalance mode only)
6. `dryRun: false` in the ConfigMap

## Configuration

Configuration is read from a ConfigMap named `longhorn-rebalancing-controller` in `longhorn-system`. The controller live-reloads it on every reconcile — no restart needed.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: longhorn-rebalancing-controller
  namespace: longhorn-system
data:
  config.yaml: |
    dryRun: true                     # safe default — flip to false when ready
    rebalance:
      nodeUsageThreshold: 75         # percent; triggers rebalance mode
      maxEvictionsPerDay: 2
      cooldownMinutes: 30
      maintenanceWindow: "02:00-05:00"
      smallestFirst: true            # evict smallest replica first (minimises rebuild time)
      graduateAfterCycles: 3         # consecutive clean cycles before entering steady-state
    steadyState:
      imbalanceRatio: 1.5            # most-loaded / least-loaded ratio threshold
      maxEvictionsPerDay: 5
      cooldownMinutes: 10
```

## Recommended rollout

1. Deploy with `dryRun: true` (the Helm chart default)
2. Watch the logs for several days — the controller logs every decision it would make
3. Set `dryRun: false` once the decisions look correct
4. The controller starts in `rebalance` mode and gradually moves replicas overnight
5. After balance is achieved, it automatically shifts to `steady-state`

## Deploying via Helm

The chart is published to Harbor as an OCI artifact.

```sh
helm registry login harbor.vollminlab.com

helm install longhorn-rebalancing-controller \
  oci://harbor.vollminlab.com/vollminlab/charts/longhorn-rebalancing-controller \
  --namespace longhorn-system
```

### Key values

| Value | Default | Description |
|---|---|---|
| `image.repository` | `harbor.vollminlab.com/vollminlab/longhorn-rebalancing-controller` | Controller image |
| `image.tag` | _(chart appVersion)_ | Image tag |
| `config.dryRun` | `true` | Set to `false` to enable actual evictions |
| `config.rebalance.nodeUsageThreshold` | `75` | Absolute disk usage % that triggers rebalance mode |
| `config.rebalance.maintenanceWindow` | `"02:00-05:00"` | Time window for rebalance evictions |
| `config.steadyState.imbalanceRatio` | `1.5` | Ratio threshold for steady-state evictions |

## RBAC

The chart creates:

- **ClusterRole / ClusterRoleBinding** — `get`, `list`, `watch` on `nodes.longhorn.io` and `volumes.longhorn.io`; `get`, `list`, `watch`, `patch` on `replicas.longhorn.io`
- **Role / RoleBinding** in `longhorn-system` — `get`, `list`, `watch` on ConfigMaps

## License

Apache 2.0 — see [LICENSE](LICENSE).
