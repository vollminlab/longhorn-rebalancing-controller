package controller

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lhv1b2 "github.com/vollminlab/longhorn-rebalancing-controller/internal/longhorn"
)

const gib = int64(1) << 30

func mkReplica(name, node, volume string, size int64) *lhv1b2.LonghornReplica {
	return &lhv1b2.LonghornReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: lhv1b2.LonghornReplicaSpec{
			NodeID:     node,
			VolumeName: volume,
			VolumeSize: fmt.Sprintf("%d", size),
			Active:     true,
		},
	}
}

// mkStats builds a nodeStats entry. availableBytes defaults to maxBytes - scheduledBytes
// unless overridden, which approximates a node whose replicas are fully written.
func mkStats(scheduled, max int64) *nodeStats {
	return &nodeStats{
		scheduledBytes: scheduled,
		maxBytes:       max,
		availableBytes: max - scheduled,
		usagePct:       float64(scheduled) / float64(max) * 100,
	}
}

func allNodesEligible(stats map[string]*nodeStats) map[string]map[string]struct{} {
	all := make(map[string]struct{}, len(stats))
	for n := range stats {
		all[n] = struct{}{}
	}
	return map[string]map[string]struct{}{"longhorn": all}
}

// TestFilterViable_AntiAffinityPingPong models the 2026-07-21 incident: the only
// destination that would pass the no-flip guard already holds the volume's other
// replica, so Longhorn would be forced to rebuild on an equally loaded node —
// the eviction must be rejected.
func TestFilterViable_AntiAffinityPingPong(t *testing.T) {
	stats := map[string]*nodeStats{
		"w01": mkStats(128*gib, 250*gib),
		"w02": mkStats(24*gib, 250*gib), // holds the peer replica
		"w03": mkStats(226*gib, 250*gib),
		"w04": mkStats(226*gib, 250*gib), // source
		"w05": mkStats(88*gib, 250*gib),
		"w06": mkStats(122*gib, 250*gib),
	}
	rep := mkReplica("minio-r1", "w04", "minio-vol", 100*gib)
	volumeNodes := map[string]map[string]struct{}{
		"minio-vol": {"w04": {}, "w02": {}},
	}
	viable := filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "w04",
		map[string]string{"minio-vol": "longhorn"}, allNodesEligible(stats),
		stats, volumeNodes, 25.0,
	)
	if len(viable) != 0 {
		t.Fatalf("expected no viable replicas (anti-affinity excludes the only balanced destination), got %d", len(viable))
	}
}

// TestFilterViable_NoFlipGuard rejects an eviction whose only destination would
// end up more loaded than the source after the move.
func TestFilterViable_NoFlipGuard(t *testing.T) {
	stats := map[string]*nodeStats{
		"src": mkStats(200*gib, 250*gib),
		"dst": mkStats(60*gib, 250*gib),
	}
	// 100G move: src drops to 100G, dst rises to 160G — flip, reject.
	rep := mkReplica("r1", "src", "vol", 100*gib)
	viable := filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "src",
		map[string]string{"vol": "longhorn"}, allNodesEligible(stats),
		stats, map[string]map[string]struct{}{"vol": {"src": {}}}, 25.0,
	)
	if len(viable) != 0 {
		t.Fatalf("expected no-flip guard to reject, got %d viable", len(viable))
	}

	// 40G move: src drops to 160G, dst rises to 100G — improves spread, allow.
	rep2 := mkReplica("r2", "src", "vol2", 40*gib)
	viable = filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep2}, "src",
		map[string]string{"vol2": "longhorn"}, allNodesEligible(stats),
		stats, map[string]map[string]struct{}{"vol2": {"src": {}}}, 25.0,
	)
	if len(viable) != 1 {
		t.Fatalf("expected 40G replica to be viable, got %d", len(viable))
	}
}

// TestFilterViable_FreeDiskFloor rejects a destination that would drop below the
// free-disk floor even though it passes the no-flip guard on scheduled bytes.
func TestFilterViable_FreeDiskFloor(t *testing.T) {
	stats := map[string]*nodeStats{
		"src": mkStats(200*gib, 250*gib),
		"dst": mkStats(40*gib, 250*gib),
	}
	// Thin-provisioned volumes elsewhere have eaten the actual free space:
	// only 80G free although just 40G is scheduled.
	stats["dst"].availableBytes = 80 * gib

	rep := mkReplica("r1", "src", "vol", 40*gib)
	// After absorbing 40G: free 40G of 250G = 16% < 25% floor.
	viable := filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "src",
		map[string]string{"vol": "longhorn"}, allNodesEligible(stats),
		stats, map[string]map[string]struct{}{"vol": {"src": {}}}, 25.0,
	)
	if len(viable) != 0 {
		t.Fatalf("expected free-disk floor to reject, got %d viable", len(viable))
	}

	// With a lower floor the same move is allowed.
	viable = filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "src",
		map[string]string{"vol": "longhorn"}, allNodesEligible(stats),
		stats, map[string]map[string]struct{}{"vol": {"src": {}}}, 10.0,
	)
	if len(viable) != 1 {
		t.Fatalf("expected replica to be viable at 10%% floor, got %d", len(viable))
	}
}

// TestFilterViable_SCEligibility rejects when the only balanced node is not
// eligible for the volume's StorageClass.
func TestFilterViable_SCEligibility(t *testing.T) {
	stats := map[string]*nodeStats{
		"src":     mkStats(200*gib, 250*gib),
		"general": mkStats(20*gib, 250*gib),
	}
	scEligibility := map[string]map[string]struct{}{
		"longhorn-dmz": {"src": {}}, // DMZ SC only eligible on the source node
	}
	rep := mkReplica("r1", "src", "dmz-vol", 40*gib)
	viable := filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "src",
		map[string]string{"dmz-vol": "longhorn-dmz"}, scEligibility,
		stats, map[string]map[string]struct{}{"dmz-vol": {"src": {}}}, 25.0,
	)
	if len(viable) != 0 {
		t.Fatalf("expected SC eligibility to reject, got %d viable", len(viable))
	}
}

// TestFilterViable_UnknownSCStillGuarded verifies an unknown SC no longer grants
// a free pass: all nodes become candidates but the guards still apply.
func TestFilterViable_UnknownSCStillGuarded(t *testing.T) {
	stats := map[string]*nodeStats{
		"src": mkStats(200*gib, 250*gib),
		"dst": mkStats(20*gib, 250*gib),
	}
	rep := mkReplica("r1", "src", "orphan-vol", 300*gib) // no destination can absorb this
	viable := filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "src",
		map[string]string{}, map[string]map[string]struct{}{},
		stats, map[string]map[string]struct{}{}, 25.0,
	)
	if len(viable) != 0 {
		t.Fatalf("expected unknown-SC replica to still be guarded, got %d viable", len(viable))
	}

	rep2 := mkReplica("r2", "src", "orphan-vol2", 40*gib)
	viable = filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep2}, "src",
		map[string]string{}, map[string]map[string]struct{}{},
		stats, map[string]map[string]struct{}{}, 25.0,
	)
	if len(viable) != 1 {
		t.Fatalf("expected unknown-SC replica with a valid destination to be viable, got %d", len(viable))
	}
}

func TestFilterViable_UnparsableSizeSkipped(t *testing.T) {
	stats := map[string]*nodeStats{
		"src": mkStats(200*gib, 250*gib),
		"dst": mkStats(20*gib, 250*gib),
	}
	rep := mkReplica("r1", "src", "vol", 0)
	rep.Spec.VolumeSize = "not-a-number"
	viable := filterViableReplicas(
		[]*lhv1b2.LonghornReplica{rep}, "src",
		map[string]string{"vol": "longhorn"}, allNodesEligible(stats),
		stats, map[string]map[string]struct{}{}, 25.0,
	)
	if len(viable) != 0 {
		t.Fatalf("expected unparsable-size replica to be skipped, got %d viable", len(viable))
	}
}

func TestBuildVolumeReplicaNodes(t *testing.T) {
	evicting := mkReplica("r3", "w03", "vol-a", 10*gib)
	evicting.Spec.EvictionRequested = true
	inactive := mkReplica("r4", "w04", "vol-a", 10*gib)
	inactive.Spec.Active = false

	list := &lhv1b2.LonghornReplicaList{Items: []lhv1b2.LonghornReplica{
		*mkReplica("r1", "w01", "vol-a", 10*gib),
		*mkReplica("r2", "w02", "vol-a", 10*gib),
		*evicting,
		*inactive,
		*mkReplica("r5", "w01", "vol-b", 5*gib),
	}}
	m := buildVolumeReplicaNodes(list)

	wantA := []string{"w01", "w02", "w03"} // eviction-pending still occupies its node
	if len(m["vol-a"]) != len(wantA) {
		t.Fatalf("vol-a nodes = %v, want %v", m["vol-a"], wantA)
	}
	for _, n := range wantA {
		if _, ok := m["vol-a"][n]; !ok {
			t.Errorf("vol-a missing node %s", n)
		}
	}
	if _, ok := m["vol-a"]["w04"]; ok {
		t.Errorf("inactive replica's node w04 must not be counted")
	}
	if len(m["vol-b"]) != 1 {
		t.Fatalf("vol-b nodes = %v, want just w01", m["vol-b"])
	}
}

func TestFindVictim_BiggestFirst(t *testing.T) {
	reps := []*lhv1b2.LonghornReplica{
		mkReplica("small", "n", "v1", 10*gib),
		mkReplica("big", "n", "v2", 100*gib),
		mkReplica("mid", "n", "v3", 50*gib),
	}
	if v := findVictim(reps, false); v.Name != "big" {
		t.Fatalf("biggest-first picked %s", v.Name)
	}
	if v := findVictim(reps, true); v.Name != "small" {
		t.Fatalf("smallest-first picked %s", v.Name)
	}
}
