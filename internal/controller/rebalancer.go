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
	evictionResetDay       int
}

type nodeStats struct {
	scheduledBytes int64
	maxBytes       int64
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

	scEligibility := buildSCEligibility(state.storageClasses, state.nodes)
	volumeToSC := buildVolumeToSC(state.volumes)
	stats := computeNodeStats(state.nodes, state.replicas)
	r.logNodeStats(ctx, stats)

	if !cfg.DryRun && !r.safetyGatesPass(ctx, state.volumes, state.replicas) {
		return ctrl.Result{RequeueAfter: r.SyncInterval}, nil
	}

	evicted, err := r.maybeEvict(ctx, cfg, stats, volumeToSC, scEligibility)
	if err != nil {
		return ctrl.Result{RequeueAfter: r.SyncInterval}, err
	}
	if evicted {
		return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
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
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	today := now.YearDay()
	if today != r.evictionResetDay {
		r.todayEvictions = 0
		r.evictionResetDay = today
	}

	r.doUpdateMode(ctx, cfg, stats)

	switch r.mode {
	case modeRebalance:
		return r.evictRebalance(ctx, cfg, stats, volumeToSC, scEligibility, now)
	case modeSteadyState:
		return r.evictSteadyState(ctx, cfg, stats, volumeToSC, scEligibility, now)
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

	viable := filterViableReplicas(stats[targetNode].replicas, targetNode, volumeToSC, scEligibility, stats)
	victim := findVictim(viable, cfg.Rebalance.SmallestFirst)
	if victim == nil {
		log.Info("no evictable replica on overloaded node", "node", targetNode)
		return false, nil
	}

	size, _ := strconv.ParseInt(victim.Spec.VolumeSize, 10, 64)
	log.Info("evicting replica (rebalance)",
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

	if err := r.setEvictionRequested(ctx, victim); err != nil {
		return false, err
	}
	r.lastEvictionTime = now
	r.todayEvictions++
	return true, nil
}

func (r *RebalancingReconciler) evictSteadyState(
	ctx context.Context,
	cfg *config.Config,
	stats map[string]*nodeStats,
	volumeToSC map[string]string,
	scEligibility map[string]map[string]struct{},
	now time.Time,
) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

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

	viable := filterViableReplicas(stats[maxNode].replicas, maxNode, volumeToSC, scEligibility, stats)
	victim := findVictim(viable, true)
	if victim == nil {
		log.Info("no evictable replica on most-loaded node", "node", maxNode)
		return false, nil
	}

	size, _ := strconv.ParseInt(victim.Spec.VolumeSize, 10, 64)
	log.Info("evicting replica (steady-state)",
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

	if err := r.setEvictionRequested(ctx, victim); err != nil {
		return false, err
	}
	r.lastEvictionTime = now
	r.todayEvictions++
	return true, nil
}

func (r *RebalancingReconciler) setEvictionRequested(ctx context.Context, replica *lhv1b2.LonghornReplica) error {
	patch := client.MergeFrom(replica.DeepCopy())
	replica.Spec.EvictionRequested = true
	return r.Client.Patch(ctx, replica, patch)
}

// filterViableReplicas returns only those replicas for which at least one eligible
// node (per SC nodeSelector) exists that is less loaded than currentNode.
// This prevents evicting replicas that can only be rebuilt on the same node.
func filterViableReplicas(
	replicas []*lhv1b2.LonghornReplica,
	currentNode string,
	volumeToSC map[string]string,
	scEligibility map[string]map[string]struct{},
	stats map[string]*nodeStats,
) []*lhv1b2.LonghornReplica {
	currentLoad := stats[currentNode].scheduledBytes
	var viable []*lhv1b2.LonghornReplica
	for _, rep := range replicas {
		scName := volumeToSC[rep.Spec.VolumeName]
		eligible, ok := scEligibility[scName]
		if !ok {
			// Unknown SC — allow eviction conservatively.
			viable = append(viable, rep)
			continue
		}
		for nodeName := range eligible {
			if nodeName == currentNode {
				continue
			}
			if s, ok := stats[nodeName]; ok && s.scheduledBytes < currentLoad {
				viable = append(viable, rep)
				break
			}
		}
	}
	return viable
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
