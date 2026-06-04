# longhorn-rebalancing-controller Overview

A Kubernetes controller that rebalances Longhorn replica placement across worker nodes based on **scheduled storage bytes**, not replica count.

## Problem

Longhorn's built-in `replica-auto-balance: best-effort` counts replicas per node, not bytes. Nodes with many large volumes become significantly more loaded than nodes with many small volumes. This controller evicts over-placed replicas so Longhorn rebuilds them on less-loaded nodes.

## Two-mode operation

**`rebalance` mode** — recovers from an imbalanced state:
- Triggers when any node exceeds the absolute `nodeUsageThreshold` (default 75%)
- Only evicts during the configured `maintenanceWindow` (default 02:00–05:00)
- Max 2 evictions/day, 30-minute cooldown
- Graduates to `steady-state` after 3 consecutive clean cycles

**`steady-state` mode** — prevents drift once balanced:
- Triggers when most-loaded node has >1.5× the bytes of least-loaded node
- No maintenance window restriction
- Max 5 evictions/day, 10-minute cooldown

## Safety gates

All must hold before any eviction:

1. All volumes cluster-wide are `robustness == healthy`
2. No replica is currently `currentState == rebuilding`
3. Cooldown period elapsed since last eviction
4. Daily eviction count below cap
5. Within maintenance window (rebalance mode only)
6. `dryRun: false` in the ConfigMap

## Rollout

Deploy with `dryRun: true` (the default). Watch logs for several days, then flip to `dryRun: false` once the decisions look correct.
