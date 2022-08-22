package scuttlebutt

import (
	"math"
	"time"
)

var getTimeNow = time.Now
var getTimeSince = time.Since

type FailureDetectorConfig struct {
	//The threshold used by the instance that triggers suspicious level.
	//Above this threshold value a node is flagged as dead.
	//The duration is GossipInterval + AcceptableHeartbeatPause + Threshold_Adjustment
	PhiThreshold float64
	//Sampling window size
	SamplingWindowSize uint
	//Minimum standard deviation used in the calculation of phi.
	//Too low value might result in too much sensitivity for sudden, but
	//normal, deviations in heartbeat inter arrival times.
	MinStdDeviation time.Duration
	//Number of lost / delayed heartbeat before considering an anomaly.
	//This margin is important to be able to survive sudden, occasional pauses
	//in heartbeat arrivals, due to for example GC or network drop or instance
	//high load.
	AcceptableHeartbeatPause time.Duration
	//First heartbeat duration used on startup when no previous heartbeat exists.
	FirstHeartbeatEstimate time.Duration
	//Grace period after which a dead node can be removed from the cluster list.
	DeadNodeGracePeriod time.Duration
}

//FailureDetectorDefaultConfig returns a default config
func FailureDetectorDefaultConfig() *FailureDetectorConfig {
	return &FailureDetectorConfig{PhiThreshold: 8.0,
		SamplingWindowSize:       1000,
		MinStdDeviation:          100 * time.Millisecond,
		AcceptableHeartbeatPause: 3000 * time.Millisecond,
		FirstHeartbeatEstimate:   1000 * time.Millisecond,
		DeadNodeGracePeriod:      1 * time.Hour}
}

type failureDetector struct {
	nodeSamples map[Node]*arrivalWindow
	config      *FailureDetectorConfig
	LiveNodes   map[Node]struct{}
	DeadNodes   map[Node]time.Time
}

func newFailureDetector(config *FailureDetectorConfig) *failureDetector {
	return &failureDetector{nodeSamples: make(map[Node]*arrivalWindow),
		config: config, LiveNodes: make(map[Node]struct{}), DeadNodes: make(map[Node]time.Time)}
}

func (f *failureDetector) reportHeartbeat(node Node) {
	if _, ok := f.nodeSamples[node]; !ok {
		f.nodeSamples[node] = newArrivalWindow(f.config.SamplingWindowSize,
			f.config.MinStdDeviation, f.config.AcceptableHeartbeatPause, f.config.FirstHeartbeatEstimate)
	}
	f.nodeSamples[node].reportHeartbeat()
}

func (f *failureDetector) phi(node Node) (float64, bool) {
	if p, ok := f.nodeSamples[node]; ok {
		return p.phi(), true
	}
	return 0, false
}

func (f *failureDetector) getLiveNodes() []Node {
	liveNodes := make([]Node, 0)
	for k := range f.LiveNodes {
		liveNodes = append(liveNodes, k)
	}
	return liveNodes
}

func (f *failureDetector) getLiveNodesCount() int {
	return len(f.LiveNodes)
}

func (f *failureDetector) getDeadNodes() []Node {
	deadNodes := make([]Node, 0)
	for k := range f.DeadNodes {
		deadNodes = append(deadNodes, k)
	}
	return deadNodes
}

func (f *failureDetector) getDeadNodesCount() int {
	return len(f.DeadNodes)
}

func (f *failureDetector) updateNodeState(node Node) {
	phi, ok := f.phi(node)
	if ok {
		if phi > f.config.PhiThreshold {
			delete(f.LiveNodes, node)
			f.DeadNodes[node] = getTimeNow()
			delete(f.nodeSamples, node)
		} else {
			f.LiveNodes[node] = struct{}{}
			delete(f.DeadNodes, node)
		}
	}
}

func (f *failureDetector) evictDeadNodes() []Node {
	nodes := make([]Node, 0)
	for node, t := range f.DeadNodes {
		if getTimeSince(t) >= f.config.DeadNodeGracePeriod {
			nodes = append(nodes, node)
		}
	}

	for _, node := range nodes {
		delete(f.DeadNodes, node)
	}
	return nodes
}

type arrivalWindow struct {
	arrivalIntervals         *arrayBoundedStats
	lastHeartbeat            time.Time
	minStdDeviation          time.Duration
	acceptableHeartbeatPause time.Duration
	firstHeartbeatEstimate   time.Duration
}

func newArrivalWindow(windowSize uint, minStdDeviation, acceptableHeartbeatPause,
	firstHeartbeatEstimate time.Duration) *arrivalWindow {
	return &arrivalWindow{arrivalIntervals: newArrayBoundedStats(windowSize),
		lastHeartbeat: time.Time{}, minStdDeviation: minStdDeviation,
		acceptableHeartbeatPause: acceptableHeartbeatPause,
		firstHeartbeatEstimate:   firstHeartbeatEstimate}
}

func (a *arrivalWindow) reportHeartbeat() {
	if !a.lastHeartbeat.IsZero() {
		interArrivalTime := getTimeSince(a.lastHeartbeat)
		a.arrivalIntervals.appendInterval(float64(interArrivalTime.Milliseconds()))
	} else {
		meanTime := a.firstHeartbeatEstimate
		stdDevTime := meanTime / 4
		a.arrivalIntervals.appendInterval(float64((meanTime - stdDevTime).Milliseconds()))
		a.arrivalIntervals.appendInterval(float64((meanTime + stdDevTime).Milliseconds()))
	}
	a.lastHeartbeat = getTimeNow()
}

func (a *arrivalWindow) phi() float64 {
	elapsedTime := float64(getTimeSince(a.lastHeartbeat).Milliseconds())
	mean := a.arrivalIntervals.meanValue() + float64(a.acceptableHeartbeatPause.Milliseconds())
	stdDev := math.Max(a.arrivalIntervals.stdDev(), float64(a.minStdDeviation.Milliseconds()))
	y := (elapsedTime - mean) / stdDev
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	if elapsedTime > mean {
		return -math.Log10(e / (1.0 + e))
	}
	return -math.Log10(1.0 - 1.0/(1.0+e))
}

type arrayBoundedStats struct {
	data     []float64
	size     uint
	isFilled bool
	index    uint
	sum      float64
	mean     float64
}

func newArrayBoundedStats(size uint) *arrayBoundedStats {
	return &arrayBoundedStats{data: make([]float64, size), size: size}
}

func (b *arrayBoundedStats) meanValue() float64 {
	return b.mean
}

func (b *arrayBoundedStats) variance() float64 {
	total := 0.0
	l := int(b.getLen())
	for i := 0; i < l; i++ {
		total += math.Pow(b.data[i], 2)
	}

	n := float64(b.getLen())
	return (total / n) - (b.mean * b.mean)
}

func (b *arrayBoundedStats) stdDev() float64 {
	return math.Sqrt(b.variance())
}

func (b *arrayBoundedStats) cdf(x float64) float64 {
	return 0.5 + 0.5*math.Erf((x-b.meanValue())/(b.stdDev()*math.Sqrt2))
}

func (b *arrayBoundedStats) appendInterval(interval float64) {
	if b.index == b.size {
		b.isFilled = true
		b.index = 0
	}

	if b.isFilled {
		b.sum -= b.data[b.index]
	}
	b.sum += interval

	b.data[b.index] = interval
	b.index++
	b.mean = b.sum / float64(b.getLen())
}

func (b *arrayBoundedStats) getLen() uint {
	if b.isFilled {
		return b.size
	}
	return b.index
}
