package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vollminlab/longhorn-rebalancing-controller/internal/config"
	lhv1b2 "github.com/vollminlab/longhorn-rebalancing-controller/internal/longhorn"
)

// Surge-move: instead of replica-level eviction (which Longhorn's NodeController
// force-reverts, see syncReplicaEvictionRequested), a move bumps the volume's
// numberOfReplicas so Longhorn schedules one extra replica on the least-loaded
// eligible node, waits for it to become healthy, deletes the SOURCE replica
// itself, and restores the spec. The controller — never Longhorn's
// cleanupAutoBalancedReplicas — picks which replica goes: on a bare spec shrink
// Longhorn chooses its own victim (ascending storageAvailable) and on
// 2026-07-22 that tore down the freshly built replica.
//
// All move state is persisted as annotations on the Volume CR so a controller
// restart mid-move resumes from the CR alone.
const (
	annMoveSource     = "rebalancer.vollminlab.com/move-source-replica"
	annMoveOriginal   = "rebalancer.vollminlab.com/move-original-replicas"
	annMoveInitial    = "rebalancer.vollminlab.com/move-initial-replicas"
	annMoveStartedAt  = "rebalancer.vollminlab.com/move-started-at"
	annMoveNewReplica = "rebalancer.vollminlab.com/move-new-replica"
)

type moveOutcome int

const (
	moveInProgress moveOutcome = iota
	moveCompleted
	moveAborted
)

// All Volume CR writes below use JSON merge patches, never full-object
// Updates: LonghornVolumeSpec is a partial type (no size field), so a full
// Update serializes spec.size as absent and Longhorn's validator.longhorn.io
// webhook denies it as a shrink ("shrinking volume ... to 2097152 is not
// supported"). A merge patch carries only the fields we changed.

// startMove begins a surge-move in one atomic patch: numberOfReplicas+1 plus
// the complete move state as annotations.
func (r *RebalancingReconciler) startMove(
	ctx context.Context,
	vol *lhv1b2.LonghornVolume,
	victim *lhv1b2.LonghornReplica,
	replicas *lhv1b2.LonghornReplicaList,
	now time.Time,
) error {
	var initial []string
	for i := range replicas.Items {
		rep := &replicas.Items[i]
		if rep.Spec.Active && rep.Spec.VolumeName == vol.Name {
			initial = append(initial, rep.Name)
		}
	}
	orig := vol.DeepCopy()
	if vol.Annotations == nil {
		vol.Annotations = map[string]string{}
	}
	vol.Annotations[annMoveSource] = victim.Name
	vol.Annotations[annMoveOriginal] = strconv.Itoa(vol.Spec.NumberOfReplicas)
	vol.Annotations[annMoveInitial] = strings.Join(initial, ",")
	vol.Annotations[annMoveStartedAt] = now.Format(time.RFC3339)
	vol.Spec.NumberOfReplicas++
	return r.Patch(ctx, vol, client.MergeFrom(orig))
}

func findInFlightMove(volumes *lhv1b2.LonghornVolumeList) *lhv1b2.LonghornVolume {
	for i := range volumes.Items {
		if _, ok := volumes.Items[i].Annotations[annMoveSource]; ok {
			return &volumes.Items[i]
		}
	}
	return nil
}

// progressMove advances an in-flight move by one step. It must run before the
// safety gates: a surged volume is deliberately degraded/rebuilding while the
// new replica syncs, and the gates would otherwise deadlock the move.
func (r *RebalancingReconciler) progressMove(
	ctx context.Context,
	cfg *config.Config,
	vol *lhv1b2.LonghornVolume,
	replicas *lhv1b2.LonghornReplicaList,
	now time.Time,
) (moveOutcome, error) {
	log := ctrl.LoggerFrom(ctx)

	source := vol.Annotations[annMoveSource]
	original, err := strconv.Atoi(vol.Annotations[annMoveOriginal])
	if err != nil {
		return moveInProgress, fmt.Errorf("move state on %s corrupt: %s=%q: %w",
			vol.Name, annMoveOriginal, vol.Annotations[annMoveOriginal], err)
	}
	startedAt, err := time.Parse(time.RFC3339, vol.Annotations[annMoveStartedAt])
	if err != nil {
		return moveInProgress, fmt.Errorf("move state on %s corrupt: %s=%q: %w",
			vol.Name, annMoveStartedAt, vol.Annotations[annMoveStartedAt], err)
	}
	initialSet := map[string]struct{}{}
	for _, n := range strings.Split(vol.Annotations[annMoveInitial], ",") {
		if n != "" {
			initialSet[n] = struct{}{}
		}
	}

	// Identify the surged replica: the active replica whose name is not in the
	// recorded pre-move set. Persist it so later reconciles track the same one.
	newName := vol.Annotations[annMoveNewReplica]
	var newRep *lhv1b2.LonghornReplica
	for i := range replicas.Items {
		rep := &replicas.Items[i]
		if rep.Spec.VolumeName != vol.Name || !rep.Spec.Active {
			continue
		}
		if newName != "" {
			if rep.Name == newName {
				newRep = rep
			}
			continue
		}
		if _, preexisting := initialSet[rep.Name]; !preexisting {
			newRep = rep
		}
	}
	if newRep != nil && newName == "" {
		orig := vol.DeepCopy()
		vol.Annotations[annMoveNewReplica] = newRep.Name
		if err := r.Patch(ctx, vol, client.MergeFrom(orig)); err != nil {
			return moveInProgress, err
		}
		log.Info("surge replica scheduled",
			"volume", vol.Name, "replica", newRep.Name, "destinationNode", newRep.Spec.NodeID)
	}

	if newRep != nil && newRep.Spec.HealthyAt != "" && newRep.Spec.FailedAt == "" && vol.Status.Robustness == "healthy" {
		return r.completeMove(ctx, vol, source, original, newRep, now.Sub(startedAt))
	}

	if now.Sub(startedAt) > time.Duration(cfg.Move.TimeoutMinutes)*time.Minute {
		return r.abortMove(ctx, vol, original, newRep)
	}
	return moveInProgress, nil
}

// completeMove deletes the source replica, then restores the spec and clears
// the move state. Delete-first ordering is deliberate: with the spec still
// surged, Longhorn has no excess healthy replicas to clean up, so it can never
// pick its own victim. The sub-second delete→update window cannot trigger a
// rebuild either: replenishReplicas requires the engine map to have dropped the
// deleted replica (hasEngineStatusSynced), which takes seconds.
func (r *RebalancingReconciler) completeMove(
	ctx context.Context,
	vol *lhv1b2.LonghornVolume,
	source string,
	original int,
	newRep *lhv1b2.LonghornReplica,
	elapsed time.Duration,
) (moveOutcome, error) {
	log := ctrl.LoggerFrom(ctx)

	src := &lhv1b2.LonghornReplica{
		ObjectMeta: metav1.ObjectMeta{Name: source, Namespace: vol.Namespace},
	}
	if err := r.Delete(ctx, src); err != nil && !apierrors.IsNotFound(err) {
		return moveInProgress, fmt.Errorf("delete source replica %s: %w", source, err)
	}
	orig := vol.DeepCopy()
	clearMoveState(vol, original)
	if err := r.Patch(ctx, vol, client.MergeFrom(orig)); err != nil {
		// Source is already gone; the next reconcile retries the restore
		// (the delete tolerates NotFound).
		return moveInProgress, err
	}
	log.Info("surge-move completed",
		"volume", vol.Name,
		"sourceReplica", source,
		"newReplica", newRep.Name,
		"destinationNode", newRep.Spec.NodeID,
		"elapsed", elapsed.Round(time.Second),
	)
	return moveCompleted, nil
}

// abortMove rolls a timed-out move back: the surged replica (if one was ever
// scheduled) is deleted, the source is kept, and the spec is restored.
func (r *RebalancingReconciler) abortMove(
	ctx context.Context,
	vol *lhv1b2.LonghornVolume,
	original int,
	newRep *lhv1b2.LonghornReplica,
) (moveOutcome, error) {
	log := ctrl.LoggerFrom(ctx)

	if newRep != nil {
		if err := r.Delete(ctx, newRep); err != nil && !apierrors.IsNotFound(err) {
			return moveInProgress, fmt.Errorf("delete surged replica %s: %w", newRep.Name, err)
		}
	}
	orig := vol.DeepCopy()
	clearMoveState(vol, original)
	if err := r.Patch(ctx, vol, client.MergeFrom(orig)); err != nil {
		return moveInProgress, err
	}
	log.Info("surge-move aborted on timeout", "volume", vol.Name, "sourceReplica", vol.Name)
	return moveAborted, nil
}

func clearMoveState(vol *lhv1b2.LonghornVolume, original int) {
	vol.Spec.NumberOfReplicas = original
	for _, ann := range []string{annMoveSource, annMoveOriginal, annMoveInitial, annMoveStartedAt, annMoveNewReplica} {
		delete(vol.Annotations, ann)
	}
}

// recordMoveOutcome updates the daily accounting: completed moves burn the
// eviction cap, aborted moves burn only the failure cap, and both start the
// cooldown clock (either way a rebuild consumed I/O).
func (r *RebalancingReconciler) recordMoveOutcome(outcome moveOutcome, now time.Time) {
	if outcome == moveInProgress {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetDailyCountersLocked(now)
	r.lastEvictionTime = now
	switch outcome {
	case moveCompleted:
		r.todayEvictions++
	case moveAborted:
		r.todayMoveFailures++
	}
}

// resetDailyCountersLocked zeroes both daily counters on the first call of a
// new day. Must be called with r.mu held.
func (r *RebalancingReconciler) resetDailyCountersLocked(now time.Time) {
	if today := now.YearDay(); today != r.evictionResetDay {
		r.todayEvictions = 0
		r.todayMoveFailures = 0
		r.evictionResetDay = today
	}
}

// filterAttachedReplicas drops replicas of volumes that are not attached:
// Longhorn's replenishReplicas only creates the surged replica while the
// volume is attached, so a move on a detached volume would just hang.
func filterAttachedReplicas(replicas []*lhv1b2.LonghornReplica, volumes *lhv1b2.LonghornVolumeList) []*lhv1b2.LonghornReplica {
	attached := make(map[string]struct{})
	for i := range volumes.Items {
		if volumes.Items[i].Status.State == "attached" {
			attached[volumes.Items[i].Name] = struct{}{}
		}
	}
	var out []*lhv1b2.LonghornReplica
	for _, rep := range replicas {
		if _, ok := attached[rep.Spec.VolumeName]; ok {
			out = append(out, rep)
		}
	}
	return out
}

func findVolume(volumes *lhv1b2.LonghornVolumeList, name string) *lhv1b2.LonghornVolume {
	for i := range volumes.Items {
		if volumes.Items[i].Name == name {
			return &volumes.Items[i]
		}
	}
	return nil
}
