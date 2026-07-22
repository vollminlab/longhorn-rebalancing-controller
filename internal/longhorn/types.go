package longhorn

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "longhorn.io", Version: "v1beta2"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypeWithName(GroupVersion.WithKind("Node"), &LonghornNode{})
	s.AddKnownTypeWithName(GroupVersion.WithKind("NodeList"), &LonghornNodeList{})
	s.AddKnownTypeWithName(GroupVersion.WithKind("Replica"), &LonghornReplica{})
	s.AddKnownTypeWithName(GroupVersion.WithKind("ReplicaList"), &LonghornReplicaList{})
	s.AddKnownTypeWithName(GroupVersion.WithKind("Volume"), &LonghornVolume{})
	s.AddKnownTypeWithName(GroupVersion.WithKind("VolumeList"), &LonghornVolumeList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

// --- Node ---

type LonghornNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LonghornNodeSpec   `json:"spec,omitempty"`
	Status            LonghornNodeStatus `json:"status,omitempty"`
}

type LonghornNodeSpec struct {
	Disks map[string]LonghornDiskSpec `json:"disks,omitempty"`
	Tags  []string                    `json:"tags,omitempty"`
}

type LonghornDiskSpec struct {
	AllowScheduling bool   `json:"allowScheduling,omitempty"`
	StorageReserved int64  `json:"storageReserved,omitempty"`
	Path            string `json:"path,omitempty"`
}

type LonghornNodeStatus struct {
	DiskStatus map[string]LonghornDiskStatus `json:"diskStatus,omitempty"`
}

type LonghornDiskStatus struct {
	StorageAvailable int64            `json:"storageAvailable,omitempty"`
	StorageScheduled int64            `json:"storageScheduled,omitempty"`
	StorageMaximum   int64            `json:"storageMaximum,omitempty"`
	ScheduledReplica map[string]int64 `json:"scheduledReplica,omitempty"`
}

func (n *LonghornNode) DeepCopyObject() runtime.Object { return n.DeepCopy() }

func (n *LonghornNode) DeepCopy() *LonghornNode {
	if n == nil {
		return nil
	}
	out := &LonghornNode{TypeMeta: n.TypeMeta}
	n.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if n.Spec.Disks != nil {
		out.Spec.Disks = make(map[string]LonghornDiskSpec, len(n.Spec.Disks))
		for k, v := range n.Spec.Disks {
			out.Spec.Disks[k] = v
		}
	}
	if n.Spec.Tags != nil {
		out.Spec.Tags = make([]string, len(n.Spec.Tags))
		copy(out.Spec.Tags, n.Spec.Tags)
	}
	if n.Status.DiskStatus != nil {
		out.Status.DiskStatus = make(map[string]LonghornDiskStatus, len(n.Status.DiskStatus))
		for k, v := range n.Status.DiskStatus {
			ds := LonghornDiskStatus{
				StorageAvailable: v.StorageAvailable,
				StorageScheduled: v.StorageScheduled,
				StorageMaximum:   v.StorageMaximum,
			}
			if v.ScheduledReplica != nil {
				ds.ScheduledReplica = make(map[string]int64, len(v.ScheduledReplica))
				for rk, rv := range v.ScheduledReplica {
					ds.ScheduledReplica[rk] = rv
				}
			}
			out.Status.DiskStatus[k] = ds
		}
	}
	return out
}

func (n *LonghornNode) DeepCopyInto(out *LonghornNode) { *out = *n.DeepCopy() }

type LonghornNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LonghornNode `json:"items"`
}

func (l *LonghornNodeList) DeepCopyObject() runtime.Object {
	if l == nil {
		return nil
	}
	out := &LonghornNodeList{TypeMeta: l.TypeMeta}
	l.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]LonghornNode, len(l.Items))
	for i := range l.Items {
		l.Items[i].DeepCopyInto(&out.Items[i])
	}
	return out
}

// --- Replica ---

type LonghornReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LonghornReplicaSpec   `json:"spec,omitempty"`
	Status            LonghornReplicaStatus `json:"status,omitempty"`
}

type LonghornReplicaSpec struct {
	NodeID            string `json:"nodeID,omitempty"`
	DiskID            string `json:"diskID,omitempty"`
	VolumeName        string `json:"volumeName,omitempty"`
	VolumeSize        string `json:"volumeSize,omitempty"`
	Active            bool   `json:"active,omitempty"`
	EvictionRequested bool   `json:"evictionRequested,omitempty"`
	// HealthyAt is set by Longhorn once the replica holds a full copy of the
	// data; FailedAt is set when the replica fails. Both live in spec, not
	// status, in Longhorn's data model.
	HealthyAt string `json:"healthyAt,omitempty"`
	FailedAt  string `json:"failedAt,omitempty"`
}

type LonghornReplicaStatus struct {
	CurrentState string `json:"currentState,omitempty"`
}

func (r *LonghornReplica) DeepCopyObject() runtime.Object { return r.DeepCopy() }

func (r *LonghornReplica) DeepCopy() *LonghornReplica {
	if r == nil {
		return nil
	}
	out := &LonghornReplica{TypeMeta: r.TypeMeta}
	r.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = r.Spec
	out.Status = r.Status
	return out
}

func (r *LonghornReplica) DeepCopyInto(out *LonghornReplica) { *out = *r.DeepCopy() }

type LonghornReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LonghornReplica `json:"items"`
}

func (l *LonghornReplicaList) DeepCopyObject() runtime.Object {
	if l == nil {
		return nil
	}
	out := &LonghornReplicaList{TypeMeta: l.TypeMeta}
	l.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]LonghornReplica, len(l.Items))
	for i := range l.Items {
		l.Items[i].DeepCopyInto(&out.Items[i])
	}
	return out
}

// --- Volume ---

type LonghornVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LonghornVolumeSpec   `json:"spec,omitempty"`
	Status            LonghornVolumeStatus `json:"status,omitempty"`
}

type LonghornVolumeSpec struct {
	NumberOfReplicas int    `json:"numberOfReplicas,omitempty"`
	StorageClassName string `json:"storageClassName,omitempty"`
}

type LonghornVolumeStatus struct {
	Robustness string `json:"robustness,omitempty"`
	State      string `json:"state,omitempty"`
}

func (v *LonghornVolume) DeepCopyObject() runtime.Object { return v.DeepCopy() }

func (v *LonghornVolume) DeepCopy() *LonghornVolume {
	if v == nil {
		return nil
	}
	out := &LonghornVolume{TypeMeta: v.TypeMeta}
	v.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = v.Spec
	out.Status = v.Status
	return out
}

func (v *LonghornVolume) DeepCopyInto(out *LonghornVolume) { *out = *v.DeepCopy() }

type LonghornVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LonghornVolume `json:"items"`
}

func (l *LonghornVolumeList) DeepCopyObject() runtime.Object {
	if l == nil {
		return nil
	}
	out := &LonghornVolumeList{TypeMeta: l.TypeMeta}
	l.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]LonghornVolume, len(l.Items))
	for i := range l.Items {
		l.Items[i].DeepCopyInto(&out.Items[i])
	}
	return out
}
