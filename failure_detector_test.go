package scuttlebutt

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
)

func float64Equal(a, b, e float64) bool {
	if a == b {
		return true
	}

	d := math.Abs(a - b)
	if b == 0 {
		return d < e
	}
	return (d / math.Abs(b)) < e
}

func TestFailureDetectorSimple(t *testing.T) {
	mockedClock := clock.NewMock()
	mockedClock.Set(time.Now())
	getTimeNow = mockedClock.Now
	getTimeSince = mockedClock.Since

	defer func() {
		getTimeNow = time.Now
		getTimeSince = time.Since
	}()

	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node3 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	intervals := []time.Duration{1, 2}
	nodes := []Node{node1, node2, node3}
	nodesID := []string{node1.ID, node2.ID, node3.ID}
	sort.Strings(nodesID)

	f := newFailureDetector(FailureDetectorDefaultConfig())
	for i := 0; i < 2000; i++ {
		randInterval := rand.Intn(len(intervals))
		timeOffset := intervals[randInterval]
		randNode := rand.Intn(len(nodes))
		node := nodes[randNode]
		mockedClock.Add(timeOffset * time.Second)
		f.reportHeartbeat(node)
	}

	for _, node := range nodes {
		f.updateNodeState(node)
	}

	var liveNodes []string
	for node := range f.LiveNodes {
		liveNodes = append(liveNodes, node.ID)
	}
	sort.Strings(liveNodes)
	require.Equal(t, nodesID, liveNodes)
	require.Equal(t, []Node{}, f.evictDeadNodes())

	mockedClock.Add(50 * time.Second)
	for _, node := range nodes {
		f.updateNodeState(node)
	}
	var deadNodes []string
	for node := range f.DeadNodes {
		deadNodes = append(deadNodes, node.ID)
	}
	sort.Strings(deadNodes)
	require.Equal(t, nodesID, deadNodes)
	require.Equal(t, []Node{}, f.evictDeadNodes())

	mockedClock.Add(25 * 60 * 60 * time.Second)
	gcNodes := f.evictDeadNodes()
	require.Equal(t, 0, len(f.LiveNodes))
	require.Equal(t, 0, len(f.DeadNodes))

	var removedNodes []string
	for _, node := range gcNodes {
		removedNodes = append(removedNodes, node.ID)
	}
	sort.Strings(removedNodes)
	require.Equal(t, nodesID, removedNodes)
}

func TestFailureDetectorSingleNodeLiveDownLive(t *testing.T) {
	mockedClock := clock.NewMock()
	mockedClock.Set(time.Now())
	getTimeNow = mockedClock.Now
	getTimeSince = mockedClock.Since

	defer func() {
		getTimeNow = time.Now
		getTimeSince = time.Since
	}()

	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	intervals := []time.Duration{1, 2}

	f := newFailureDetector(FailureDetectorDefaultConfig())
	for i := 0; i < 2000; i++ {
		randInterval := rand.Intn(len(intervals))
		timeOffset := intervals[randInterval]
		mockedClock.Add(timeOffset * time.Second)
		f.reportHeartbeat(node1)
	}

	f.updateNodeState(node1)

	var liveNodes []string
	for node := range f.LiveNodes {
		liveNodes = append(liveNodes, node.ID)
	}
	require.Equal(t, []string{node1.ID}, liveNodes)
	require.Equal(t, []Node{}, f.evictDeadNodes())

	mockedClock.Add(20 * time.Second)
	f.updateNodeState(node1)
	require.Equal(t, 0, len(f.LiveNodes))
	for i := 0; i < 500; i++ {
		randInterval := rand.Intn(len(intervals))
		timeOffset := intervals[randInterval]
		mockedClock.Add(timeOffset * time.Second)
		f.reportHeartbeat(node1)
	}

	liveNodes = liveNodes[:0]
	f.updateNodeState(node1)
	for node := range f.LiveNodes {
		liveNodes = append(liveNodes, node.ID)
	}
	require.Equal(t, []string{node1.ID}, liveNodes)
	require.Equal(t, []Node{}, f.evictDeadNodes())
}

func TestFailureDetectorSingleNodeAfterInitialInterval(t *testing.T) {
	mockedClock := clock.NewMock()
	mockedClock.Set(time.Now())
	getTimeNow = mockedClock.Now
	getTimeSince = mockedClock.Since

	defer func() {
		getTimeNow = time.Now
		getTimeSince = time.Since
	}()

	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	f := newFailureDetector(FailureDetectorDefaultConfig())
	f.reportHeartbeat(node1)

	mockedClock.Add(1 * time.Second)
	f.updateNodeState(node1)

	var liveNodes []string
	for node := range f.LiveNodes {
		liveNodes = append(liveNodes, node.ID)
	}
	require.Equal(t, []string{node1.ID}, liveNodes)
	require.Equal(t, []Node{}, f.evictDeadNodes())
	mockedClock.Add(40 * time.Second)
	f.updateNodeState(node1)
	require.Equal(t, 0, len(f.LiveNodes))
}

func TestBoundedArrayStats(t *testing.T) {
	b := newArrayBoundedStats(10)
	for i := 1; i < 10; i++ {
		b.appendInterval(float64(i))
	}
	require.Equal(t, uint(9), b.index)
	require.Equal(t, uint(9), b.getLen())
	require.Equal(t, false, b.isFilled)
	require.Equal(t, true, float64Equal(b.meanValue(), 5.0, 1e-12))

	for i := 10; i < 14; i++ {
		b.appendInterval(float64(i))
	}
	require.Equal(t, uint(3), b.index)
	require.Equal(t, uint(10), b.getLen())
	require.Equal(t, true, b.isFilled)
	require.Equal(t, true, float64Equal(b.meanValue(), 8.5, 1e-12))
}

func TestArrivalWindow(t *testing.T) {
	mockedClock := clock.NewMock()
	mockedClock.Set(time.Now())
	getTimeNow = mockedClock.Now
	getTimeSince = mockedClock.Since

	defer func() {
		getTimeNow = time.Now
		getTimeSince = time.Since
	}()

	s := newArrivalWindow(10, 5*time.Second, 0, 2*time.Second)
	s.reportHeartbeat()

	mockedClock.Add(3 * time.Second)
	s.reportHeartbeat()

	require.Equal(t, true, float64Equal(s.phi(), 0.167637554559269, 1e-12))

	mockedClock.Add(1 * time.Second)
	require.Equal(t, true, float64Equal(s.phi(), 0.21806933966441244, 1e-12))

	mockedClock.Add(5 * time.Second)
	s.reportHeartbeat()
	mockedClock.Add(2 * time.Second)
	require.Equal(t, true, float64Equal(s.phi(), 0.22271211050155057, 1e-12))

	f := newArrivalWindow(3, 5*time.Second, 0, 1*time.Second)
	f.reportHeartbeat()
	mockedClock.Add(1 * time.Second)
	f.reportHeartbeat()
	mockedClock.Add(2 * time.Second)
	f.reportHeartbeat()
	mockedClock.Add(3 * time.Second)
	f.reportHeartbeat()
	require.Equal(t, 2.0, f.arrivalIntervals.meanValue()/1000)
	require.Equal(t, 0.8164965809277261, f.arrivalIntervals.stdDev()/1000)
	require.Equal(t, 0.666666666666667, f.arrivalIntervals.variance()/1000/1000)

}
