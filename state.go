package scuttlebutt

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"golang.org/x/exp/slices"
)

const (
	maxKeySize   = 1024
	maxValueSize = 4096
)

var (
	errMaxKeySizeExceeded   = fmt.Errorf("max key size (%d) has been exceeded", maxKeySize)
	errMaxValueSizeExceeded = fmt.Errorf("max value size (%d) has been exceeded", maxValueSize)
)

//Node represents a node in the cluster
type Node struct {
	//The name of the node which must be unique in the cluster.
	ID string
	//Gossip public address to advertise to other cluster members.
	GossipPublicAddress string
}

type nodeState struct {
	KV            map[string]versionedValue
	lastHeartbeat time.Time
	maxVersion    version
}

func newNodeState() *nodeState {
	return &nodeState{KV: make(map[string]versionedValue),
		lastHeartbeat: getTimeNow(), maxVersion: 0}
}

func (ns *nodeState) getMaxVersion() version {
	return ns.maxVersion
}

func (ns *nodeState) get(key string) (string, bool) {
	if v, ok := ns.KV[key]; ok {
		return v.Value, true
	}
	return "", false
}

func (ns *nodeState) getVersioned(key string) (*versionedValue, bool) {
	if v, ok := ns.KV[key]; ok {
		return &v, true
	}
	return nil, false
}

func (ns *nodeState) set(key, value string) error {
	newVersion := ns.maxVersion + 1
	return ns.setWithVersion(key, value, newVersion)
}

func (ns *nodeState) setWithVersion(key, value string, valueVersion version) error {
	if len(key) > maxKeySize {
		return errMaxKeySizeExceeded
	}

	if len(value) > maxValueSize {
		return errMaxValueSizeExceeded
	}
	ns.maxVersion = valueVersion
	ns.KV[key] = versionedValue{Value: value, Version: valueVersion}
	return nil
}

func (ns *nodeState) staleKeyValues(floorVersion version) map[string]versionedValue {
	m := make(map[string]versionedValue)
	for k, v := range ns.KV {
		if v.Version > floorVersion {
			m[k] = v
		}
	}
	return m
}

func (ns *nodeState) staleKeyValuesCount(floorVersion version) int {
	var count int
	for _, v := range ns.KV {
		if v.Version > floorVersion {
			count++
		}
	}
	return count
}

type kvPair struct {
	Key   string
	Value versionedValue
}

func (ns *nodeState) staleKeyValuesSlice(floorVersion version) []kvPair {
	m := make([]kvPair, 0)
	for k, v := range ns.KV {
		if v.Version > floorVersion {
			m = append(m, kvPair{k, v})
		}
	}
	sort.SliceStable(m, func(i, j int) bool {
		return m[i].Value.Version < m[j].Value.Version
	})
	return m
}

type clusterState struct {
	NodeStates map[Node]*nodeState
	SeedNodes  []string
}

type clusterStateSnapshot struct {
	NodeStates map[Node]nodeState
	SeedNodes  []string
}

func newClusterState() *clusterState {
	return &clusterState{NodeStates: make(map[Node]*nodeState),
		SeedNodes: make([]string, 0)}
}

func (cs *clusterState) getNodeStateDefault(node Node) *nodeState {
	if n, ok := cs.NodeStates[node]; ok {
		return n
	}
	ns := newNodeState()
	cs.NodeStates[node] = ns
	return ns
}

func (cs *clusterState) getNodeState(node Node) (*nodeState, bool) {
	n, ok := cs.NodeStates[node]
	return n, ok
}

func (cs *clusterState) getNodes() []Node {
	n := make([]Node, 0)
	for k := range cs.NodeStates {
		n = append(n, k)
	}
	return n
}

func (cs *clusterState) getNodesCount() int {
	return len(cs.NodeStates)
}

func (cs *clusterState) removeNode(node Node) {
	if _, ok := cs.NodeStates[node]; ok {
		delete(cs.NodeStates, node)
	}
}

func (cs *clusterState) applyDelta(delta *delta) map[Node]struct{} {
	nodeUpdates := make(map[Node]struct{}, 0)
	for node, nodedelta := range delta.NodeDeltas {
		needsUpdate := false
		if _, ok := cs.getNodeState(node); !ok {
			needsUpdate = true
		} else {
			if len(cs.NodeStates[node].KV) == 0 {
				needsUpdate = true
			}
		}
		nodestate := cs.getNodeStateDefault(node)

		for key, vvalue := range nodedelta.KV {
			if vvalue.Version > nodestate.maxVersion {
				nodestate.maxVersion = vvalue.Version
			}
			if e, ok := nodestate.KV[key]; ok {
				if e.Version >= vvalue.Version {
					continue
				}
				nodestate.KV[key] = vvalue
				if key != heartbeatKey && !needsUpdate {
					nodeUpdates[node] = struct{}{}
				}
			} else {
				nodestate.KV[key] = vvalue
				if key != heartbeatKey && !needsUpdate {
					nodeUpdates[node] = struct{}{}
				}
			}
		}
		nodestate.lastHeartbeat = getTimeNow()
	}
	return nodeUpdates
}

func (cs *clusterState) computeDigest(deadNodes []Node) *digest {
	digest := newDigest()
	for node, nodestate := range cs.NodeStates {
		if deadNodes != nil && slices.Contains(deadNodes, node) {
			continue
		}
		digest.addNode(node, nodestate.maxVersion)
	}
	return digest
}

func (cs *clusterState) computeRandomDigest(mtu int, deadNodes []Node) *digest {
	ns := make([]Node, 0)
	for node := range cs.NodeStates {
		ns = append(ns, node)
	}

	n := len(ns)
	rand.Shuffle(n, func(i, j int) {
		ns[i], ns[j] = ns[j], ns[i]
	})

	digestWriter := newDigestWriter(mtu)
	for _, node := range ns {
		if deadNodes != nil && slices.Contains(deadNodes, node) {
			continue
		}
		st := cs.NodeStates[node]
		if !digestWriter.addNode(node, st.maxVersion) {
			break
		}
	}
	return digestWriter.getDigest()
}

//scuttle-depth ordering
func (cs *clusterState) computeDelta(mtu int, digest *digest, deadNodes []Node) *delta {
	sortedNodes := make(reverseSortedNodes)
	deltaWriter := newDeltaWriter(mtu)
	for node, nodestate := range cs.NodeStates {
		if deadNodes != nil && slices.Contains(deadNodes, node) {
			continue
		}
		floorVersion, _ := digest.getNodeMaxVersion(node)
		staleCount := nodestate.staleKeyValuesCount(floorVersion)
		if staleCount > 0 {
			if _, ok := sortedNodes[staleCount]; !ok {
				sortedNodes[staleCount] = make([]Node, 0)
			}
			sortedNodes[staleCount] = append(sortedNodes[staleCount], node)
		}
	}

loop:
	for _, index := range sortedNodes.sort() {
		for _, node := range sortedNodes[index] {
			if !deltaWriter.addNode(node) {
				break loop
			}
			nodestate := cs.NodeStates[node]
			floorVersion, _ := digest.getNodeMaxVersion(node)
			staleKV := nodestate.staleKeyValuesSlice(floorVersion)
			for _, e := range staleKV {
				if !deltaWriter.addKV(e.Key, e.Value) {
					return deltaWriter.getDelta()
				}
			}
		}
	}
	return deltaWriter.getDelta()
}

func (cs *clusterState) snapshot() *clusterStateSnapshot {
	css := &clusterStateSnapshot{NodeStates: make(map[Node]nodeState),
		SeedNodes: make([]string, len(cs.SeedNodes))}

	copy(css.SeedNodes, cs.SeedNodes)
	for node, nodestate := range cs.NodeStates {
		css.NodeStates[node] = *nodestate
	}
	return css
}
