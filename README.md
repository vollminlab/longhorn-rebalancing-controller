# longhorn-rebalancing-controller

A Kubernetes controller that rebalances [Longhorn](https://longhorn.io) replica placement across worker nodes based on scheduled storage bytes — not just replica count.

Longhorn's built-in `replica-auto-balance: best-effort` only counts replicas per node, not bytes. This causes nodes with many large volumes to become significantly more loaded than nodes with many small volumes. This controller fills that gap by moving over-placed replicas onto less-loaded nodes.

## How it works

The controller watches `nodes.longhorn.io` and `replicas.longhorn.io` in `longhorn-system`. Every 5 minutes (and on any node or replica change) it computes scheduled storage bytes per node and decides whether to move a replica.

### Surge-move

A move never reduces redundancy. The controller:

1. **Surges** — bumps the volume's `spec.numberOfReplicas` by one; Longhorn schedules the extra replica on the least-loaded eligible node (the controller's destination guards have already verified one exists). Move state is persisted as annotations on the Volume CR, so a controller restart mid-move resumes cleanly.
2. **Waits** — polls every 30 s until the new replica reports `healthyAt` and the volume returns to `robustness: healthy`. Only one move runs at a time.
3. **Completes** — deletes the *source* replica itself, then restores `numberOfReplicas`. The controller does the delete (never a bare spec shrink) because Longhorn's own excess-replica cleanup picks its victim by node free space and can tear down the freshly built replica instead.

A move that exceeds `move.timeoutMinutes` (default 90) is rolled back: the surged replica is deleted, the source is kept, and the failure counts against `move.maxFailuresPerDay` — not the daily move cap, which only completed moves consume.

> Replica-level `spec.evictionRequested` (the v0.2.0 mechanism) is not used: Longhorn's NodeController owns that flag and force-reverts any external write to it (`EvictionCanceled`).

### Two-mode operation

**`rebalance` mode** — recovers from an imbalanced state:
- Triggers when any node exceeds the absolute `nodeUsageThreshold` (default 75%)
- Only starts moves during the configured `maintenanceWindow` (default `02:00-05:00`); a move already in flight is allowed to finish outside it
- Conservative: max 2 completed moves/day, 30-minute cooldown between moves
- Graduates to `steady-state` after 3 consecutive check cycles with all nodes below threshold

**`steady-state` mode** — prevents drift once balanced:
- Triggers when the most-loaded node has >1.5× the scheduled bytes of the least-loaded node
- No maintenance window restriction — reacts promptly to small imbalances
- Max 5 completed moves/day, 10-minute cooldown
- Reverts to `rebalance` if any node crosses the absolute threshold again

### Safety gates

All of the following must hold before any new move is started:

1. All volumes cluster-wide have `robustness == healthy`
2. No replica is currently in `currentState == rebuilding`
3. No other move in flight
4. The volume is attached (Longhorn only builds the surged replica on attached volumes)
5. Cooldown period elapsed since the last move
6. Completed-move count below the daily cap, aborted-move count below `move.maxFailuresPerDay`
7. Within maintenance window (rebalance mode only)
8. `dryRun: false` in the ConfigMap

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
      smallestFirst: true            # move smallest replica first (minimises rebuild time)
      graduateAfterCycles: 3         # consecutive clean cycles before entering steady-state
    steadyState:
      imbalanceRatio: 1.5            # most-loaded / least-loaded ratio threshold
      maxEvictionsPerDay: 5
      cooldownMinutes: 10
    move:
      timeoutMinutes: 90             # abort a surge-move older than this
      maxFailuresPerDay: 3           # aborted moves before new moves stop for the day
      perVolumeBackoffMinutes: 360   # keep a moved volume off the victim list this long
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
| `config.dryRun` | `true` | Set to `false` to enable actual moves |
| `config.rebalance.nodeUsageThreshold` | `75` | Absolute disk usage % that triggers rebalance mode |
| `config.rebalance.maintenanceWindow` | `"02:00-05:00"` | Time window for starting rebalance moves |
| `config.steadyState.imbalanceRatio` | `1.5` | Ratio threshold for steady-state moves |
| `config.move.timeoutMinutes` | `90` | Age after which an in-flight move is aborted |
| `config.move.maxFailuresPerDay` | `3` | Aborted moves before new moves stop for the day |
| `config.move.perVolumeBackoffMinutes` | `360` | How long a moved volume is kept off the victim list (0 disables) |

## RBAC

The chart creates:

- **ClusterRole / ClusterRoleBinding** — `get`, `list`, `watch` on `nodes.longhorn.io`; `get`, `list`, `watch`, `update`, `patch` on `volumes.longhorn.io` (surge + move annotations); `get`, `list`, `watch`, `patch`, `delete` on `replicas.longhorn.io` (source-replica removal)
- **Role / RoleBinding** in `longhorn-system` — `get`, `list`, `watch` on ConfigMaps

## License

Apache 2.0 — see [LICENSE](LICENSE).
