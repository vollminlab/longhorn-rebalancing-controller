package controller

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/vollminlab/longhorn-rebalancing-controller/internal/config"
	lhv1b2 "github.com/vollminlab/longhorn-rebalancing-controller/internal/longhorn"
)

const (
	modeRebalance       = "rebalance"
	modeSteadyState     = "steady-state"
	longhornNS          = "longhorn-system"
	longhornProvisioner = "driver.longhorn.io"
	revertHysteresis    = 7.0
)

type RebalancingReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	ConfigMapName      string
	ConfigMapNamespace string
	SyncInterval       time.Duration

	mu                     sync.Mutex
	mode                   string
	consecutiveCleanCycles int
	lastEvictionTime       time.Time
	todayEvictions         int
	todayMoveFailures      int
	evictionResetDay       int
	// lastMoveByVolume records when each volume was last surge-moved, so a single
	// volume cannot be shuffled again within Move.PerVolumeBackoffMinutes. This is
	// belt-and-suspenders on top of the peak-reduction guard: the guard already
	// makes moves converge, and the backoff stops any one volume from absorbing
	// repeated moves during that convergence.
	lastMoveByVolume map[string]time.Time
}

type nodeStats struct {
	scheduledBytes int64
	maxBytes       int64
	availableBytes int64
	usagePct       float64
	replicas       []*lhv1b2.LonghornReplica
}

type clusterState struct {
	nodes          *lhv1b2.LonghornNodeList
	replicas       *lhv1b2.LonghornReplicaList
	volumes        *lhv1b2.LonghornVolumeList
	storageClasses *storagev1.StorageClassList
}

func (r *RebalancingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	cfg, err := r.loadConfig(ctx)
	if err != nil {
		log.Error(err, "config load failed, using defaults")
		cfg = config.Default()
	}

	state, err := r.loadClusterState(ctx)
	if err != nil {
		return ctrl.Result{RequeueAfter: r.SyncInterval}, err
	}

	// An in-flight surge-move is progressed before anything else — the surged
	// volume is deliberately degraded while its new replica rebuilds, so the
	// safety gates below would deadlock the move. No new move starts while one
	// is running.
	if inFlight := findInFlightMove(state.volumes); inFlight != nil {
		now := time.Now()
		outcome, err := r.progressMove(ctx, cfg, inFlight, state.replicas, now)
		if err != nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
		r.recordMoveOutcome(outcome, now)
		if outcome == moveInProgress {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{RequeueAfter: r.SyncInterval}, nil
	}

	scEligibility := buildSCEligibility(state.storageClasses, state.nodes)
	volumeToSC := buildVolumeToSC(state.volumes)
	volumeNodes := buildVolumeReplicaNodes(state.replicas)
	stats := computeNodeStats(state.nodes, state.replicas)
	r.logNodeStats(ctx, stats)

	if !cfg.DryRun && !r.safetyGatesPass(ctx, state.volumes, state.replicas) {
		return ctrl.Result{RequeueAfter: r.SyncInterval}, nil
	}

	moved, err := r.maybeEvict(ctx, cfg, stats, volumeToSC, scEligibility, volumeNodes, state.volumes, state.replicas)
	if err != nil {
		return ctrl.Result{RequeueAfter: r.SyncInterval}, err
	}
	if moved {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: r.SyncInterval}, nil
}

func (r *RebalancingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	toGlobal := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, _ client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "cluster"}}}
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("longhorn-rebalancing").
		For(&lhv1b2.LonghornNode{}).
		Watches(&lhv1b2.LonghornReplica{}, toGlobal).
		Watches(&corev1.ConfigMap{}, toGlobal, builder.WithPredicates(
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetName() == r.ConfigMapName && obj.GetNamespace() == r.ConfigMapNamespace
			}),
		)).
		Complete(r)
}

func (r *RebalancingReconciler) loadConfig(ctx context.Context) (*config.Config, error) {
	var cm corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Name: r.ConfigMapName, Namespace: r.ConfigMapNamespace}, &cm)
	if apierrors.IsNotFound(err) {
		return config.Default(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("get configmap: %w", err)
	}
	data, ok := cm.Data["config.yaml"]
	if !ok {
		return config.Default(), nil
	}
	return config.ParseYAML([]byte(data))
}

func (r *RebalancingReconciler) loadClusterState(ctx context.Context) (*clusterState, error) {
	var nodes lhv1b2.LonghornNodeList
	if err := r.List(ctx, &nodes, client.InNamespace(longhornNS)); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var replicas lhv1b2.LonghornReplicaList
	if err := r.List(ctx, &replicas, client.InNamespace(longhornNS)); err != nil {
		return nil, fmt.Errorf("list replicas: %w", err)
	}
	var volumes lhv1b2.LonghornVolumeList
	if err := r.List(ctx, &volumes, client.InNamespace(longhornNS)); err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}
	var storageClasses storagev1.StorageClassList
	if err := r.List(ctx, &storageClasses); err != nil {
		return nil, fmt.Errorf("list storageclasses: %w", err)
	}
	return &clusterState{
		nodes:          &nodes,
		replicas:       &replicas,
		volumes:        &volumes,
		storageClasses: &storageClasses,
	}, nil
}

// buildSCEligibility maps each Longhorn StorageClass name to the set of node names
// whose Longhorn tags satisfy the SC's nodeSelector parameter.
// SCs without a nodeSelector are eligible on all nodes.
func buildSCEligibility(storageClasses *storagev1.StorageClassList, nodes *lhv1b2.LonghornNodeList) map[string]map[string]struct{} {
	nodeTags := make(map[string]map[string]struct{}, len(nodes.Items))
	for i := range nodes.Items {
		n := &nodes.Items[i]
		tags := make(map[string]struct{}, len(n.Spec.Tags))
		for _, t := range n.Spec.Tags {
			tags[t] = struct{}{}
		}
		nodeTags[n.Name] = tags
	}

	allNodes := make(map[string]struct{}, len(nodes.Items))
	for i := range nodes.Items {
		allNodes[nodes.Items[i].Name] = struct{}{}
	}

	result := make(map[string]map[string]struct{})
	for i := range storageClasses.Items {
		sc := &storageClasses.Items[i]
		if sc.Provisioner != longhornProvisioner {
			continue
		}
		tag := sc.Parameters["nodeSelector"]
		if tag == "" {
			result[sc.Name] = allNodes
		} else {
			eligible := make(map[string]struct{})
			for nodeName, tags := range nodeTags {
				if _, ok := tags[tag]; ok {
					eligible[nodeName] = struct{}{}
				}
			}
			result[sc.Name] = eligible
		}
	}
	return result
}

func buildVolumeToSC(volumes *lhv1b2.LonghornVolumeList) map[string]string {
	m := make(map[string]string, len(volumes.Items))
	for i := range volumes.Items {
		v := &volumes.Items[i]
		m[v.Name] = v.Spec.StorageClassName
	}
	return m
}

// buildVolumeReplicaNodes maps each volume to the set of nodes currently holding
// one of its active replicas. Replicas pending eviction are included: their data
// stays on the node until the rebuild completes, so Longhorn's replica
// anti-affinity still excludes that node as a rebuild destination.
func buildVolumeReplicaNodes(replicas *lhv1b2.LonghornReplicaList) map[string]map[string]struct{} {
	m := make(map[string]map[string]struct{})
	for i := range replicas.Items {
		rep := &replicas.Items[i]
		if !rep.Spec.Active || rep.Spec.NodeID == "" {
			continue
		}
		nodes, ok := m[rep.Spec.VolumeName]
		if !ok {
			nodes = make(map[string]struct{})
			m[rep.Spec.VolumeName] = nodes
		}
		nodes[rep.Spec.NodeID] = struct{}{}
	}
	return m
}

// computeNodeStats aggregates per-node scheduled/max bytes and associates active replicas.
// Replicas with EvictionRequested=true have their size subtracted from scheduledBytes
// to reflect the space Longhorn will free once eviction completes.
func computeNodeStats(nodes *lhv1b2.LonghornNodeList, replicas *lhv1b2.LonghornReplicaList) map[string]*nodeStats {
	stats := make(map[string]*nodeStats, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		s := &nodeStats{}
		for diskID, disk := range node.Status.DiskStatus {
			spec, ok := node.Spec.Disks[diskID]
			if !ok || !spec.AllowScheduling {
				continue
			}
			s.scheduledBytes += disk.StorageScheduled
			s.maxBytes += disk.StorageMaximum
			s.availableBytes += disk.StorageAvailable
		}
		stats[node.Name] = s
	}

	for i := range replicas.Items {
		rep := &replicas.Items[i]
		if !rep.Spec.Active {
			continue
		}
		s, ok := stats[rep.Spec.NodeID]
		if !ok {
			continue
		}
		if rep.Spec.EvictionRequested {
			// Subtract pending-eviction bytes so we don't double-count freed space.
			if size, err := strconv.ParseInt(rep.Spec.VolumeSize, 10, 64); err == nil {
				s.scheduledBytes -= size
				if s.scheduledBytes < 0 {
					s.scheduledBytes = 0
				}
			}
			continue
		}
		s.replicas = append(s.replicas, rep)
	}

	for _, s := range stats {
		if s.maxBytes > 0 {
			s.usagePct = float64(s.scheduledBytes) / float64(s.maxBytes) * 100
		}
	}
	return stats
}

func (r *RebalancingReconciler) logNodeStats(ctx context.Context, stats map[string]*nodeStats) {
	log := ctrl.LoggerFrom(ctx)
	r.mu.Lock()
	mode := r.mode
	if mode == "" {
		mode = modeRebalance
	}
	r.mu.Unlock()

	for name, s := range stats {
		log.Info("node",
			"node", name,
			"scheduledGiB", s.scheduledBytes>>30,
			"maxGiB", s.maxBytes>>30,
			"usagePct", fmt.Sprintf("%.1f%%", s.usagePct),
			"replicas", len(s.replicas),
			"mode", mode,
		)
	}
}

func (r *RebalancingReconciler) safetyGatesPass(ctx context.Context, volumes *lhv1b2.LonghornVolumeList, replicas *lhv1b2.LonghornReplicaList) bool {
	log := ctrl.LoggerFrom(ctx)

	for i := range volumes.Items {
		v := &volumes.Items[i]
		rob := v.Status.Robustness
		if rob == "degraded" || rob == "faulted" {
			log.Info("volume not healthy — skipping eviction", "volume", v.Name, "robustness", rob)
			return false
		}
	}

	for i := range replicas.Items {
		rep := &replicas.Items[i]
		if rep.Status.CurrentState == "rebuilding" {
			log.Info("replica rebuilding — skipping eviction", "replica", rep.Name, "volume", rep.Spec.VolumeName)
			return false
		}
	}

	return true
}

func (r *RebalancingReconciler) maybeEvict(
	ctx context.Context,
	cfg *config.Config,
	stats map[string]*nodeStats,
	volumeToSC map[string]string,
	scEligibility map[string]map[string]struct{},
	volumeNodes map[string]map[string]struct{},
	vols *lhv1b2.LonghornVolumeList,
	reps *lhv1b2.LonghornReplicaList,
) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	r.resetDailyCountersLocked(now)

	r.doUpdateMode(ctx, cfg, stats)

	if !cfg.DryRun && r.todayMoveFailures >= cfg.Move.MaxFailuresPerDay {
		log.Info("move failure cap reached — no new moves today",
			"failures", r.todayMoveFailures, "cap", cfg.Move.MaxFailuresPerDay)
		return false, nil
	}

	switch r.mode {
	case modeRebalance:
		return r.evictRebalance(ctx, cfg, stats, volumeToSC, scEligibility, volumeNodes, vols, reps, now)
	case modeSteadyState:
		return r.evictSteadyState(ctx, cfg, stats, volumeToSC, scEligibility, volumeNodes, vols, reps, now)
	}
	return false, nil
}

// doUpdateMode transitions between rebalance and steady-state modes.
// Must be called with r.mu held.
func (r *RebalancingReconciler) doUpdateMode(ctx context.Context, cfg *config.Config, stats map[string]*nodeStats) {
	log := ctrl.LoggerFrom(ctx)

	if r.mode == "" {
		r.mode = modeRebalance
	}

	_, maxPct := mostLoadedNode(stats)

	switch r.mode {
	case modeRebalance:
		if maxPct < cfg.Rebalance.NodeUsageThreshold {
			r.consecutiveCleanCycles++
			if r.consecutiveCleanCycles >= cfg.Rebalance.GraduateAfterCycles {
				r.mode = modeSteadyState
				r.consecutiveCleanCycles = 0
				log.Info("graduated to steady-state mode", "maxUsagePct", fmt.Sprintf("%.1f%%", maxPct))
			}
		} else {
			r.consecutiveCleanCycles = 0
		}
	case modeSteadyState:
		// Revert only when usage is meaningfully above threshold to prevent oscillation.
		if maxPct >= cfg.Rebalance.NodeUsageThreshold+revertHysteresis {
			r.mode = modeRebalance
			r.consecutiveCleanCycles = 0
			log.Info("reverted to rebalance mode", "maxUsagePct", fmt.Sprintf("%.1f%%", maxPct))
		}
	}
}

func (r *RebalancingReconciler) evictRebalance(
	ctx context.Context,
	cfg *config.Config,
	stats map[string]*nodeStats,
	volumeToSC map[string]string,
	scEligibility map[string]map[string]struct{},
	volumeNodes map[string]map[string]struct{},
	vols *lhv1b2.LonghornVolumeList,
	reps *lhv1b2.LonghornReplicaList,
	now time.Time,
) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	window, _ := config.ParseWindow(cfg.Rebalance.MaintenanceWindow)
	if !window.Contains(now) {
		log.Info("outside maintenance window", "window", cfg.Rebalance.MaintenanceWindow)
		return false, nil
	}

	if !cfg.DryRun {
		cooldown := time.Duration(cfg.Rebalance.CooldownMinutes) * time.Minute
		if !r.lastEvictionTime.IsZero() && now.Sub(r.lastEvictionTime) < cooldown {
			log.Info("cooldown active", "remaining", (cooldown - now.Sub(r.lastEvictionTime)).Round(time.Minute))
			return false, nil
		}
		if r.todayEvictions >= cfg.Rebalance.MaxEvictionsPerDay {
			log.Info("daily cap reached", "cap", cfg.Rebalance.MaxEvictionsPerDay)
			return false, nil
		}
	}

	targetNode, maxPct := mostLoadedNode(stats)
	if targetNode == "" || maxPct < cfg.Rebalance.NodeUsageThreshold {
		return false, nil
	}

	viable := filterViableReplicas(stats[targetNode].replicas, targetNode, volumeToSC, scEligibility, stats, volumeNodes, cfg.MinDestinationFreePct)
	viable = filterAttachedReplicas(viable, vols)
	viable = r.filterVolumeBackoff(viable, cfg, now)
	victim := findVictim(viable, cfg.Rebalance.SmallestFirst)
	if victim == nil {
		log.Info("no movable replica on overloaded node", "node", targetNode)
		return false, nil
	}

	size, _ := strconv.ParseInt(victim.Spec.VolumeSize, 10, 64)
	log.Info("starting surge-move (rebalance)",
		"replica", victim.Name,
		"node", targetNode,
		"volumeName", victim.Spec.VolumeName,
		"sizeGiB", size>>30,
		"nodeUsagePct", fmt.Sprintf("%.1f%%", maxPct),
		"dryRun", cfg.DryRun,
	)

	if cfg.DryRun {
		return false, nil
	}

	vol := findVolume(vols, victim.Spec.VolumeName)
	if vol == nil {
		return false, fmt.Errorf("volume %s for victim replica %s not found", victim.Spec.VolumeName, victim.Name)
	}
	if err := r.startMove(ctx, vol, victim, reps, now); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RebalancingReconciler) evictSteadyState(
	ctx context.Context,
	cfg *config.Config,
	stats map[string]*nodeStats,
	volumeToSC map[string]string,
	scEligibility map[string]map[string]struct{},
	volumeNodes map[string]map[string]struct{},
	vols *lhv1b2.LonghornVolumeList,
	reps *lhv1b2.LonghornReplicaList,
	now time.Time,
) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	// Steady-state honours the same window as evictRebalance. It did not until
	// now, which is the whole of #20: maintenanceWindow exists to keep replica
	// movement out of the nightly backup window, and this path ignored it while
	// the eviction path above respected it. Its own log made that plain, seconds
	// apart on 2026-08-22:
	//
	//   01:15:58 outside maintenance window
	//   01:16:14 starting surge-move (steady-state)
	//
	// A surge-move is a full replica rebuild. Landing one inside the backup
	// window is exactly what maintenanceWindow was added to prevent, and on this
	// cluster it also lands on saturated storage — moves fired at 00:00 and
	// 01:16 contributed to a datastore stall that took three nodes NotReady.
	//
	// Checked before the DryRun block, matching evictRebalance, so a dry run
	// reports the same decision the real path would take.
	window, _ := config.ParseWindow(cfg.Rebalance.MaintenanceWindow)
	if !window.Contains(now) {
		log.Info("outside maintenance window (steady-state)", "window", cfg.Rebalance.MaintenanceWindow)
		return false, nil
	}

	if !cfg.DryRun {
		cooldown := time.Duration(cfg.SteadyState.CooldownMinutes) * time.Minute
		if !r.lastEvictionTime.IsZero() && now.Sub(r.lastEvictionTime) < cooldown {
			return false, nil
		}
		if r.todayEvictions >= cfg.SteadyState.MaxEvictionsPerDay {
			log.Info("daily cap reached (steady-state)", "cap", cfg.SteadyState.MaxEvictionsPerDay)
			return false, nil
		}
	}

	maxBytes, minBytes, maxNode, _ := byteExtremes(stats)
	if minBytes == 0 {
		return false, nil
	}

	ratio := float64(maxBytes) / float64(minBytes)
	if ratio <= cfg.SteadyState.ImbalanceRatio {
		return false, nil
	}

	viable := filterViableReplicas(stats[maxNode].replicas, maxNode, volumeToSC, scEligibility, stats, volumeNodes, cfg.MinDestinationFreePct)
	viable = filterAttachedReplicas(viable, vols)
	viable = r.filterVolumeBackoff(viable, cfg, now)
	victim := findVictim(viable, true)
	if victim == nil {
		log.Info("no movable replica on most-loaded node", "node", maxNode)
		return false, nil
	}

	size, _ := strconv.ParseInt(victim.Spec.VolumeSize, 10, 64)
	log.Info("starting surge-move (steady-state)",
		"replica", victim.Name,
		"node", maxNode,
		"volumeName", victim.Spec.VolumeName,
		"sizeGiB", size>>30,
		"ratio", fmt.Sprintf("%.2f", ratio),
		"dryRun", cfg.DryRun,
	)

	if cfg.DryRun {
		return false, nil
	}

	vol := findVolume(vols, victim.Spec.VolumeName)
	if vol == nil {
		return false, fmt.Errorf("volume %s for victim replica %s not found", victim.Spec.VolumeName, victim.Name)
	}
	if err := r.startMove(ctx, vol, victim, reps, now); err != nil {
		return false, err
	}
	return true, nil
}

// filterViableReplicas returns only those replicas whose eviction has at least one
// realistic destination that improves the balance. A destination is realistic when
// it is SC-eligible (per nodeSelector), is not the current node, and does not
// already hold a replica of the same volume — Longhorn's replica anti-affinity
// excludes those nodes, so counting them as destinations models a move Longhorn
// will never make. Each realistic destination must also pass two guards:
//
//   - peak-reduction: after the move, the cluster's highest node load (scheduled
//     bytes) must strictly decrease. This is what makes the controller converge —
//     every accepted move lowers the global maximum, so the sequence terminates
//     and cannot oscillate. It supersedes the earlier pairwise "no-flip" guard,
//     which structurally forbade relieving a node whose largest replica exceeded
//     any other node's headroom (e.g. the 100 GiB MinIO replica): no single
//     destination could stay below the source's post-move load, so that replica
//     could never move and the controller churned a smaller volume forever
//     without relieving the hot node. Peak-reduction also rejects the tied-max
//     case — moving off one of two equally hottest nodes leaves the twin at the
//     peak, so nothing improves — which the no-flip guard would have allowed.
//   - free-disk floor: absorbing the replica must leave the destination with at
//     least minFreePct of its Longhorn disk capacity actually free, so a rebuild
//     cannot push a node toward disk-space alerts.
//
// Longhorn, not this controller, picks the actual rebuild destination — but
// postMoveMaxLoad is monotone in destination load, so if any destination lowers
// the peak, the least-loaded one (Longhorn's preference) does too.
func filterViableReplicas(
	replicas []*lhv1b2.LonghornReplica,
	currentNode string,
	volumeToSC map[string]string,
	scEligibility map[string]map[string]struct{},
	stats map[string]*nodeStats,
	volumeNodes map[string]map[string]struct{},
	minFreePct float64,
) []*lhv1b2.LonghornReplica {
	globalMax := maxScheduled(stats)
	var viable []*lhv1b2.LonghornReplica
	for _, rep := range replicas {
		size, err := strconv.ParseInt(rep.Spec.VolumeSize, 10, 64)
		if err != nil {
			continue // can't model the move without a size
		}
		// Unknown SC behaves like an SC without nodeSelector: every node is a
		// candidate, but the guards below still apply.
		eligible := scEligibility[volumeToSC[rep.Spec.VolumeName]]
		peers := volumeNodes[rep.Spec.VolumeName]
		for nodeName, s := range stats {
			if nodeName == currentNode || s.maxBytes == 0 {
				continue
			}
			if eligible != nil {
				if _, ok := eligible[nodeName]; !ok {
					continue
				}
			}
			if _, held := peers[nodeName]; held {
				continue // anti-affinity: Longhorn won't rebuild here
			}
			if postMoveMaxLoad(stats, currentNode, nodeName, size) >= globalMax {
				continue // peak-reduction: the move must strictly lower the cluster max
			}
			if float64(s.availableBytes-size)/float64(s.maxBytes)*100 < minFreePct {
				continue // free-disk floor
			}
			viable = append(viable, rep)
			break
		}
	}
	return viable
}

// maxScheduled returns the highest scheduledBytes across all nodes.
func maxScheduled(stats map[string]*nodeStats) int64 {
	var m int64
	for _, s := range stats {
		if s.scheduledBytes > m {
			m = s.scheduledBytes
		}
	}
	return m
}

// postMoveMaxLoad returns what the cluster's highest node load would be after
// moving a replica of the given size from src to dst — src loses the bytes, dst
// gains them, every other node is unchanged.
func postMoveMaxLoad(stats map[string]*nodeStats, src, dst string, size int64) int64 {
	var maxLoad int64
	for n, s := range stats {
		load := s.scheduledBytes
		switch n {
		case src:
			load -= size
		case dst:
			load += size
		}
		if load > maxLoad {
			maxLoad = load
		}
	}
	return maxLoad
}

// filterVolumeBackoff drops replicas whose volume was surge-moved within the
// per-volume backoff window, so no single volume is moved twice in quick
// succession. A non-positive PerVolumeBackoffMinutes disables the filter.
func (r *RebalancingReconciler) filterVolumeBackoff(
	replicas []*lhv1b2.LonghornReplica,
	cfg *config.Config,
	now time.Time,
) []*lhv1b2.LonghornReplica {
	if cfg.Move.PerVolumeBackoffMinutes <= 0 || len(r.lastMoveByVolume) == 0 {
		return replicas
	}
	backoff := time.Duration(cfg.Move.PerVolumeBackoffMinutes) * time.Minute
	var kept []*lhv1b2.LonghornReplica
	for _, rep := range replicas {
		if last, ok := r.lastMoveByVolume[rep.Spec.VolumeName]; ok && now.Sub(last) < backoff {
			continue
		}
		kept = append(kept, rep)
	}
	return kept
}

func mostLoadedNode(stats map[string]*nodeStats) (string, float64) {
	var name string
	var maxPct float64
	for n, s := range stats {
		if s.usagePct > maxPct {
			maxPct = s.usagePct
			name = n
		}
	}
	return name, maxPct
}

func byteExtremes(stats map[string]*nodeStats) (maxBytes, minBytes int64, maxNode, minNode string) {
	first := true
	for n, s := range stats {
		if first {
			maxBytes, minBytes = s.scheduledBytes, s.scheduledBytes
			maxNode, minNode = n, n
			first = false
			continue
		}
		if s.scheduledBytes > maxBytes {
			maxBytes = s.scheduledBytes
			maxNode = n
		}
		if s.scheduledBytes < minBytes {
			minBytes = s.scheduledBytes
			minNode = n
		}
	}
	return
}

func findVictim(replicas []*lhv1b2.LonghornReplica, smallestFirst bool) *lhv1b2.LonghornReplica {
	if len(replicas) == 0 {
		return nil
	}
	best := replicas[0]
	bestSize, _ := strconv.ParseInt(best.Spec.VolumeSize, 10, 64)

	for _, rep := range replicas[1:] {
		size, err := strconv.ParseInt(rep.Spec.VolumeSize, 10, 64)
		if err != nil {
			continue
		}
		if (smallestFirst && size < bestSize) || (!smallestFirst && size > bestSize) {
			best = rep
			bestSize = size
		}
	}
	return best
}
