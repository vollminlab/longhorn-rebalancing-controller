package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/vollminlab/longhorn-rebalancing-controller/internal/config"
	lhv1b2 "github.com/vollminlab/longhorn-rebalancing-controller/internal/longhorn"
)

func moveScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := lhv1b2.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// moveClient builds a fake client that rejects full-object Updates on Volume
// CRs, mimicking Longhorn's validator.longhorn.io admission webhook: our
// LonghornVolumeSpec is partial (no size field), so a full-object Update
// serializes size as absent and the webhook denies it as a shrink to zero.
// The controller must only ever Patch volumes.
func moveClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(moveScheme(t)).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, isVolume := obj.(*lhv1b2.LonghornVolume); isVolume {
					return errors.New(`admission webhook "validator.longhorn.io" denied the request: shrinking volume ` +
						obj.GetName() + ` size from 107374182400 to 2097152 is not supported`)
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
}

func mkVolume(name string, numReplicas int) *lhv1b2.LonghornVolume {
	return &lhv1b2.LonghornVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: longhornNS},
		Spec: lhv1b2.LonghornVolumeSpec{
			NumberOfReplicas: numReplicas,
			StorageClassName: "longhorn",
		},
		Status: lhv1b2.LonghornVolumeStatus{State: "attached", Robustness: "healthy"},
	}
}

// mkNSReplica is mkReplica plus the longhorn-system namespace, for fake-client tests.
func mkNSReplica(name, node, volume string, size int64) *lhv1b2.LonghornReplica {
	rep := mkReplica(name, node, volume, size)
	rep.Namespace = longhornNS
	return rep
}

func getVolume(t *testing.T, c client.Client, name string) *lhv1b2.LonghornVolume {
	t.Helper()
	var vol lhv1b2.LonghornVolume
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: longhornNS}, &vol); err != nil {
		t.Fatalf("get volume %s: %v", name, err)
	}
	return &vol
}

// annotateMove marks vol as mid-move the way startMove would have.
func annotateMove(vol *lhv1b2.LonghornVolume, source string, original int, initial []string, startedAt time.Time) {
	if vol.Annotations == nil {
		vol.Annotations = map[string]string{}
	}
	vol.Annotations[annMoveSource] = source
	vol.Annotations[annMoveOriginal] = "2"
	_ = original // the tests always use 2 originals; keep signature honest
	vol.Annotations[annMoveInitial] = strings.Join(initial, ",")
	vol.Annotations[annMoveStartedAt] = startedAt.Format(time.RFC3339)
	vol.Spec.NumberOfReplicas = original + 1
}

// TestStartMove_SurgesSpecAndRecordsState verifies the move begins with a single
// atomic update: numberOfReplicas+1 plus the full move state as annotations, so
// a controller restart mid-move can resume from the CR alone.
func TestStartMove_SurgesSpecAndRecordsState(t *testing.T) {
	vol := mkVolume("pvc-a", 2)
	r1 := mkNSReplica("pvc-a-r-1", "w03", "pvc-a", 100*gib)
	r2 := mkNSReplica("pvc-a-r-2", "w02", "pvc-a", 100*gib)
	c := moveClient(t, vol, r1, r2)
	rec := &RebalancingReconciler{Client: c}

	now := time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC)
	replicas := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{*r1, *r2}}
	if err := rec.startMove(context.Background(), getVolume(t, c, "pvc-a"), r1, replicas, now); err != nil {
		t.Fatalf("startMove: %v", err)
	}

	got := getVolume(t, c, "pvc-a")
	if got.Spec.NumberOfReplicas != 3 {
		t.Errorf("numberOfReplicas = %d, want 3", got.Spec.NumberOfReplicas)
	}
	if got.Annotations[annMoveSource] != "pvc-a-r-1" {
		t.Errorf("source annotation = %q, want pvc-a-r-1", got.Annotations[annMoveSource])
	}
	if got.Annotations[annMoveOriginal] != "2" {
		t.Errorf("original annotation = %q, want 2", got.Annotations[annMoveOriginal])
	}
	if got.Annotations[annMoveStartedAt] != now.Format(time.RFC3339) {
		t.Errorf("startedAt annotation = %q, want %s", got.Annotations[annMoveStartedAt], now.Format(time.RFC3339))
	}
	initial := strings.Split(got.Annotations[annMoveInitial], ",")
	if len(initial) != 2 {
		t.Fatalf("initial replicas = %v, want both pre-existing names", initial)
	}
	seen := map[string]bool{}
	for _, n := range initial {
		seen[n] = true
	}
	if !seen["pvc-a-r-1"] || !seen["pvc-a-r-2"] {
		t.Errorf("initial replicas = %v, want pvc-a-r-1 and pvc-a-r-2", initial)
	}
}

func TestFindInFlightMove(t *testing.T) {
	idle := mkVolume("idle", 2)
	moving := mkVolume("moving", 3)
	moving.Annotations = map[string]string{annMoveSource: "moving-r-1"}

	vols := &lhv1b2.LonghornVolumeList{Items: []lhv1b2.LonghornVolume{*idle, *moving}}
	if got := findInFlightMove(vols); got == nil || got.Name != "moving" {
		t.Fatalf("findInFlightMove = %v, want the annotated volume", got)
	}

	vols = &lhv1b2.LonghornVolumeList{Items: []lhv1b2.LonghornVolume{*idle}}
	if got := findInFlightMove(vols); got != nil {
		t.Fatalf("findInFlightMove = %v, want nil", got)
	}
}

// TestProgressMove_IdentifiesNewReplica: the surged replica appears as an active
// replica whose name is not in the recorded initial set; its name is persisted
// so later reconciles (or a restarted controller) track the same replica.
func TestProgressMove_IdentifiesNewReplica(t *testing.T) {
	now := time.Date(2026, 7, 23, 13, 10, 0, 0, time.UTC)
	vol := mkVolume("pvc-a", 2)
	annotateMove(vol, "pvc-a-r-1", 2, []string{"pvc-a-r-1", "pvc-a-r-2"}, now.Add(-5*time.Minute))
	vol.Status.Robustness = "degraded" // rebuild in progress — expected mid-move

	r1 := mkNSReplica("pvc-a-r-1", "w03", "pvc-a", 100*gib)
	r2 := mkNSReplica("pvc-a-r-2", "w02", "pvc-a", 100*gib)
	rNew := mkNSReplica("pvc-a-r-3", "w05", "pvc-a", 100*gib) // not yet healthy

	c := moveClient(t, vol, r1, r2, rNew)
	rec := &RebalancingReconciler{Client: c}
	replicas := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{*r1, *r2, *rNew}}

	outcome, err := rec.progressMove(context.Background(), config.Default(), getVolume(t, c, "pvc-a"), replicas, now)
	if err != nil {
		t.Fatalf("progressMove: %v", err)
	}
	if outcome != moveInProgress {
		t.Fatalf("outcome = %v, want moveInProgress", outcome)
	}
	if got := getVolume(t, c, "pvc-a").Annotations[annMoveNewReplica]; got != "pvc-a-r-3" {
		t.Errorf("new-replica annotation = %q, want pvc-a-r-3", got)
	}
}

// TestProgressMove_CompletesByDeletingSourceAndRestoringSpec: when the new
// replica is healthy the controller deletes the SOURCE replica itself, then
// restores the spec — never letting Longhorn's cleanupAutoBalancedReplicas pick
// its own victim (on 2026-07-22 that cleanup tore down the NEW replica).
func TestProgressMove_CompletesByDeletingSourceAndRestoringSpec(t *testing.T) {
	now := time.Date(2026, 7, 23, 13, 30, 0, 0, time.UTC)
	vol := mkVolume("pvc-a", 2)
	annotateMove(vol, "pvc-a-r-1", 2, []string{"pvc-a-r-1", "pvc-a-r-2"}, now.Add(-25*time.Minute))

	r1 := mkNSReplica("pvc-a-r-1", "w03", "pvc-a", 100*gib)
	r2 := mkNSReplica("pvc-a-r-2", "w02", "pvc-a", 100*gib)
	rNew := mkNSReplica("pvc-a-r-3", "w05", "pvc-a", 100*gib)
	rNew.Spec.HealthyAt = "2026-07-23T13:28:00Z"

	c := moveClient(t, vol, r1, r2, rNew)
	rec := &RebalancingReconciler{Client: c}
	replicas := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{*r1, *r2, *rNew}}

	outcome, err := rec.progressMove(context.Background(), config.Default(), getVolume(t, c, "pvc-a"), replicas, now)
	if err != nil {
		t.Fatalf("progressMove: %v", err)
	}
	if outcome != moveCompleted {
		t.Fatalf("outcome = %v, want moveCompleted", outcome)
	}

	var gone lhv1b2.LonghornReplica
	err = c.Get(context.Background(), types.NamespacedName{Name: "pvc-a-r-1", Namespace: longhornNS}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Errorf("source replica still exists (err=%v), want deleted", err)
	}

	got := getVolume(t, c, "pvc-a")
	if got.Spec.NumberOfReplicas != 2 {
		t.Errorf("numberOfReplicas = %d, want restored to 2", got.Spec.NumberOfReplicas)
	}
	for _, ann := range []string{annMoveSource, annMoveOriginal, annMoveInitial, annMoveStartedAt, annMoveNewReplica} {
		if _, ok := got.Annotations[ann]; ok {
			t.Errorf("annotation %s not cleared", ann)
		}
	}
}

// TestProgressMove_TimeoutAborts: past the timeout with an unhealthy new
// replica, the move is rolled back — new replica deleted, source kept, spec
// restored — and reported as aborted (failure cap, not eviction cap).
func TestProgressMove_TimeoutAborts(t *testing.T) {
	now := time.Date(2026, 7, 23, 15, 0, 0, 0, time.UTC)
	vol := mkVolume("pvc-a", 2)
	annotateMove(vol, "pvc-a-r-1", 2, []string{"pvc-a-r-1", "pvc-a-r-2"}, now.Add(-2*time.Hour))

	r1 := mkNSReplica("pvc-a-r-1", "w03", "pvc-a", 100*gib)
	r2 := mkNSReplica("pvc-a-r-2", "w02", "pvc-a", 100*gib)
	rNew := mkNSReplica("pvc-a-r-3", "w05", "pvc-a", 100*gib) // still not healthy

	c := moveClient(t, vol, r1, r2, rNew)
	rec := &RebalancingReconciler{Client: c}
	replicas := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{*r1, *r2, *rNew}}

	outcome, err := rec.progressMove(context.Background(), config.Default(), getVolume(t, c, "pvc-a"), replicas, now)
	if err != nil {
		t.Fatalf("progressMove: %v", err)
	}
	if outcome != moveAborted {
		t.Fatalf("outcome = %v, want moveAborted", outcome)
	}

	var gone lhv1b2.LonghornReplica
	err = c.Get(context.Background(), types.NamespacedName{Name: "pvc-a-r-3", Namespace: longhornNS}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Errorf("new replica still exists (err=%v), want deleted on abort", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-a-r-1", Namespace: longhornNS}, &gone); err != nil {
		t.Errorf("source replica must survive an abort: %v", err)
	}

	got := getVolume(t, c, "pvc-a")
	if got.Spec.NumberOfReplicas != 2 {
		t.Errorf("numberOfReplicas = %d, want restored to 2", got.Spec.NumberOfReplicas)
	}
	if _, ok := got.Annotations[annMoveSource]; ok {
		t.Error("move annotations not cleared on abort")
	}
}

// TestProgressMove_TimeoutWithHealthyNewReplicaCompletes: a timed-out move whose
// new replica turned healthy anyway completes instead of throwing the work away.
func TestProgressMove_TimeoutWithHealthyNewReplicaCompletes(t *testing.T) {
	now := time.Date(2026, 7, 23, 15, 0, 0, 0, time.UTC)
	vol := mkVolume("pvc-a", 2)
	annotateMove(vol, "pvc-a-r-1", 2, []string{"pvc-a-r-1", "pvc-a-r-2"}, now.Add(-2*time.Hour))

	r1 := mkNSReplica("pvc-a-r-1", "w03", "pvc-a", 100*gib)
	r2 := mkNSReplica("pvc-a-r-2", "w02", "pvc-a", 100*gib)
	rNew := mkNSReplica("pvc-a-r-3", "w05", "pvc-a", 100*gib)
	rNew.Spec.HealthyAt = "2026-07-23T14:55:00Z"

	c := moveClient(t, vol, r1, r2, rNew)
	rec := &RebalancingReconciler{Client: c}
	replicas := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{*r1, *r2, *rNew}}

	outcome, err := rec.progressMove(context.Background(), config.Default(), getVolume(t, c, "pvc-a"), replicas, now)
	if err != nil {
		t.Fatalf("progressMove: %v", err)
	}
	if outcome != moveCompleted {
		t.Fatalf("outcome = %v, want moveCompleted", outcome)
	}
}

// TestRecordMoveOutcome_CapOnlyOnCompletion: completed moves burn the daily
// eviction cap; aborted moves burn only the separate failure cap. Both start
// the cooldown clock (pacing rebuild I/O either way).
func TestRecordMoveOutcome_CapOnlyOnCompletion(t *testing.T) {
	now := time.Date(2026, 7, 23, 13, 30, 0, 0, time.UTC)
	rec := &RebalancingReconciler{evictionResetDay: now.YearDay()}

	rec.recordMoveOutcome(moveCompleted, now)
	if rec.todayEvictions != 1 || rec.todayMoveFailures != 0 {
		t.Fatalf("after complete: evictions=%d failures=%d, want 1/0", rec.todayEvictions, rec.todayMoveFailures)
	}
	if !rec.lastEvictionTime.Equal(now) {
		t.Fatalf("lastEvictionTime = %v, want %v", rec.lastEvictionTime, now)
	}

	later := now.Add(time.Hour)
	rec.recordMoveOutcome(moveAborted, later)
	if rec.todayEvictions != 1 || rec.todayMoveFailures != 1 {
		t.Fatalf("after abort: evictions=%d failures=%d, want 1/1", rec.todayEvictions, rec.todayMoveFailures)
	}
	if !rec.lastEvictionTime.Equal(later) {
		t.Fatalf("abort must still start cooldown; lastEvictionTime = %v", rec.lastEvictionTime)
	}

	rec.recordMoveOutcome(moveInProgress, later.Add(time.Minute))
	if rec.todayEvictions != 1 || rec.todayMoveFailures != 1 || !rec.lastEvictionTime.Equal(later) {
		t.Fatal("in-progress outcome must not change any counter")
	}

	// Day rollover resets both counters before recording.
	rec.evictionResetDay = now.YearDay() - 1
	rec.recordMoveOutcome(moveCompleted, later.Add(2*time.Minute))
	if rec.todayEvictions != 1 || rec.todayMoveFailures != 0 {
		t.Fatalf("after rollover: evictions=%d failures=%d, want 1/0", rec.todayEvictions, rec.todayMoveFailures)
	}
}

// TestFilterAttachedReplicas: surge replenishment only runs while a volume is
// attached (volume_controller.go replenishReplicas), so detached volumes must
// never be selected for a move.
func TestFilterAttachedReplicas(t *testing.T) {
	attached := mkVolume("vol-attached", 2)
	detached := mkVolume("vol-detached", 2)
	detached.Status.State = "detached"
	vols := &lhv1b2.LonghornVolumeList{Items: []lhv1b2.LonghornVolume{*attached, *detached}}

	reps := []*lhv1b2.LonghornReplica{
		mkReplica("r-a", "w03", "vol-attached", 10*gib),
		mkReplica("r-d", "w03", "vol-detached", 10*gib),
	}
	got := filterAttachedReplicas(reps, vols)
	if len(got) != 1 || got[0].Name != "r-a" {
		t.Fatalf("filterAttachedReplicas = %v, want only r-a", got)
	}
}

// TestReconcile_ProgressesInFlightMoveDespiteDegradedVolume: a surged volume is
// deliberately degraded while the new replica rebuilds — the safety gates that
// block NEW moves must not block progressing the in-flight one, and no second
// move may start while one is running.
func TestReconcile_ProgressesInFlightMoveDespiteDegradedVolume(t *testing.T) {
	startedAt := time.Now().Add(-5 * time.Minute)
	vol := mkVolume("pvc-a", 2)
	annotateMove(vol, "pvc-a-r-1", 2, []string{"pvc-a-r-1", "pvc-a-r-2"}, startedAt)
	vol.Status.Robustness = "degraded"

	r1 := mkNSReplica("pvc-a-r-1", "w03", "pvc-a", 100*gib)
	r2 := mkNSReplica("pvc-a-r-2", "w02", "pvc-a", 100*gib)
	rNew := mkNSReplica("pvc-a-r-3", "w05", "pvc-a", 100*gib)

	node := &lhv1b2.LonghornNode{ObjectMeta: metav1.ObjectMeta{Name: "w03", Namespace: longhornNS}}
	c := moveClient(t, vol, r1, r2, rNew, node)
	rec := &RebalancingReconciler{
		Client:             c,
		ConfigMapName:      "longhorn-rebalancing-controller",
		ConfigMapNamespace: longhornNS,
		SyncInterval:       5 * time.Minute,
	}

	res, err := rec.Reconcile(context.Background(), ctrl.Request{})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Fatalf("RequeueAfter = %v, want 30s while a move is in flight", res.RequeueAfter)
	}
	if got := getVolume(t, c, "pvc-a").Annotations[annMoveNewReplica]; got != "pvc-a-r-3" {
		t.Errorf("in-flight move was not progressed: new-replica annotation = %q", got)
	}
}

// TestMaybeEvict_FailureCapBlocksNewMoves: once maxMoveFailuresPerDay aborts
// have happened, no further moves start that day — the 07-21/07-22 windows
// burned the whole cap on doomed evictions.
func TestMaybeEvict_FailureCapBlocksNewMoves(t *testing.T) {
	vol := mkVolume("pvc-a", 1)
	r1 := mkNSReplica("pvc-a-r-1", "src", "pvc-a", 40*gib)
	c := moveClient(t, vol, r1)

	rec := &RebalancingReconciler{
		Client:            c,
		todayMoveFailures: 3,
		evictionResetDay:  time.Now().YearDay(),
	}

	cfg := config.Default()
	cfg.DryRun = false
	cfg.Rebalance.MaintenanceWindow = "00:00-23:59"
	stats := map[string]*nodeStats{
		"src": mkStats(200*gib, 250*gib),
		"dst": mkStats(20*gib, 250*gib),
	}
	stats["src"].replicas = []*lhv1b2.LonghornReplica{r1}

	vols := &lhv1b2.LonghornVolumeList{Items: []lhv1b2.LonghornVolume{*vol}}
	reps := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{*r1}}
	evicted, err := rec.maybeEvict(context.Background(), cfg, stats,
		map[string]string{"pvc-a": "longhorn"}, allNodesEligible(stats),
		map[string]map[string]struct{}{"pvc-a": {"src": {}}}, vols, reps)
	if err != nil {
		t.Fatalf("maybeEvict: %v", err)
	}
	if evicted {
		t.Fatal("maybeEvict started a move despite the failure cap")
	}
	if got := getVolume(t, c, "pvc-a"); got.Spec.NumberOfReplicas != 1 {
		t.Fatalf("volume was surged (replicas=%d) despite the failure cap", got.Spec.NumberOfReplicas)
	}
}
