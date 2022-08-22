/*
scuttlebutt is a library for distributed cluster membership and failure
detection via an efficient reconciliation and flow-control anti-entropy
gossip protocol.

The failure detection mechanism is based on the phi accrual failure detector
used to mark failing nodes and remove them from the membership.

The speed of convergence can be tuned via the phi accrual failure detector.
*/
package scuttlebutt

import (
	"container/list"
	"crypto/aes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/armon/go-metrics"
	"golang.org/x/exp/slices"
)

const (
	defaultGossipNodes = 3
	heartbeatKey       = "heartbeat"
	packetQueueDepth   = 4096
	protocolVersion    = 1
	udpTxBufferSize    = 65507
)

var (
	errRemotePacketEncryption = errors.New("Remote packet is encrypted and encryption is not configured")
	errSecretKey              = errors.New("Secret key must be 16, 24 or 32 bytes")
)

//State represents the current cluster state
type State struct {
	state     *clusterStateSnapshot
	liveNodes []Node
	deadNodes []Node
}

//Scuttlebutt is a scuttlebutt instance returned by NewScuttlebutt
type Scuttlebutt struct {
	config          *Config
	clusterState    *clusterState
	heartbeat       uint64
	failureDetector *failureDetector
	done            chan struct{}
	mutex           sync.Mutex
	wg              sync.WaitGroup
	logger          *log.Logger
	net             *netTransport

	queueCh     chan struct{}
	packetQueue *list.List
	queueLock   sync.Mutex

	notifyCh    chan struct{}
	notifyQueue *list.List
	notifyLock  sync.Mutex

	shutdownLock sync.Mutex
	shutdown     int32

	encryptionEnabled bool
}

//NewScuttlebutt creates a Scuttlebutt instance based on a provided config
func NewScuttlebutt(config *Config, metadata map[string]string) (*Scuttlebutt, error) {
	logger := config.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if config.GossipNodes == 0 {
		config.GossipNodes = defaultGossipNodes
	}

	nt, err := newNetTransport(config, logger)
	if err != nil {
		return nil, err
	}

	sb := &Scuttlebutt{config: config,
		clusterState:    newClusterState(),
		heartbeat:       0,
		failureDetector: newFailureDetector(config.FailureDetectorConfig),
		done:            make(chan struct{}),
		queueCh:         make(chan struct{}),
		notifyCh:        make(chan struct{}),
		logger:          logger,
		packetQueue:     list.New(),
		notifyQueue:     list.New(),
		net:             nt}

	if config.SecretKey != nil && len(config.SecretKey) > 0 {
		if keyLen := len(config.SecretKey); keyLen != 16 && keyLen != 24 && keyLen != 32 {
			return nil, errSecretKey
		}
		sb.encryptionEnabled = true
	}

	nodeState := sb.getSelfNodeState()
	nodeState.set(heartbeatKey, strconv.FormatUint(0, 10))
	for key, value := range metadata {
		nodeState.set(key, value)
	}

	sb.wg.Add(4)
	go sb.packetListen()
	go sb.notifyHandler()
	go sb.packetHandler()
	go sb.gossipHandler()

	return sb, nil
}

func (sb *Scuttlebutt) ingestPacket(pkt *udpPacket) {
	sb.mutex.Lock()
	defer sb.mutex.Unlock()

	msgType, buf, err := sb.processPacket(pkt.Buf)
	if err != nil {
		sb.logger.Printf("[ERR] failed to process packet: %v", err)
		return
	}

	switch msgType {
	case synMsg:
		fallthrough
	case ackMsg:
		sb.queuePacket(&udpPacket{Buf: buf, From: pkt.From})
	}

	return
}

func (sb *Scuttlebutt) queuePacket(pkt *udpPacket) {
	sb.queueLock.Lock()
	if sb.packetQueue.Len() >= packetQueueDepth {
		sb.logger.Printf("[WARN] packet queue full, dropping message from %s", pkt.From.String())
	} else {
		sb.packetQueue.PushBack(pkt)
	}
	sb.queueLock.Unlock()

	select {
	case sb.queueCh <- struct{}{}:
	default:
	}

	return
}

func (sb *Scuttlebutt) getQueuePacket() (*udpPacket, bool) {
	sb.queueLock.Lock()
	defer sb.queueLock.Unlock()

	if q := sb.packetQueue.Back(); q != nil {
		sb.packetQueue.Remove(q)
		packet := q.Value.(*udpPacket)
		return packet, true
	}
	return &udpPacket{}, false
}

func (sb *Scuttlebutt) packetHandler() {
	defer sb.wg.Done()
	for {
		select {
		case <-sb.done:
			return
		case <-sb.queueCh:
			for {
				packet, ok := sb.getQueuePacket()
				if !ok {
					break
				}
				n, err := sb.net.writeToUDP(packet.Buf, packet.From)
				if err != nil {
					if s := sb.net.isClosing(); s == 0 {
						sb.logger.Printf("[ERR] error writing UDP packet: %v", err)
					}
					continue
				}

				if n != len(packet.Buf) {
					sb.logger.Printf("[ERR] write error, expected %d, written %d", len(packet.Buf), n)
				}
			}
		}
	}
}

func (sb *Scuttlebutt) getQueueEvent() (*NodeEvent, bool) {
	sb.notifyLock.Lock()
	defer sb.notifyLock.Unlock()

	if n := sb.notifyQueue.Len(); n > 128 {
		sb.logger.Printf("[WARN] notify queue has exceeded 128 pending items (%d)", n)
	}

	if q := sb.notifyQueue.Back(); q != nil {
		sb.notifyQueue.Remove(q)
		event := q.Value.(*NodeEvent)
		return event, true
	}
	return &NodeEvent{}, false
}

func (sb *Scuttlebutt) notifyHandler() {
	defer sb.wg.Done()
	for {
		select {
		case <-sb.done:
			return
		case <-sb.notifyCh:
			for {
				event, ok := sb.getQueueEvent()
				if !ok {
					break
				}

				switch event.Event {
				case NodeLive:
					sb.config.Events.NotifyLive(event.Node)
				case NodeDead:
					sb.config.Events.NotifyDead(event.Node)
				case NodeUpdate:
					sb.config.Events.NotifyUpdate(event.Node)
				}
			}
		}
	}
}

func (sb *Scuttlebutt) packetListen() {
	defer sb.wg.Done()
	for {
		select {
		case <-sb.done:
			return
		case pkt := <-sb.net.packetCh():
			sb.ingestPacket(pkt)
		}
	}
}

func (sb *Scuttlebutt) gossipHandler() {
	defer sb.wg.Done()
	ticker := time.NewTicker(sb.config.GossipInterval)
	for {
		select {
		case <-ticker.C:
			sb.gossip()
		case <-sb.done:
			ticker.Stop()
			return
		}
	}
}

func (sb *Scuttlebutt) gossipOne(addr string) error {
	sb.mutex.Lock()
	defer sb.mutex.Unlock()

	synPacket, err := sb.createSynPacket()
	if err != nil {
		return err
	}

	s, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return err
	}

	sb.queueLock.Lock()
	if sb.packetQueue.Len() >= packetQueueDepth {
		sb.logger.Printf("[WARN] packet queue full, dropping SYN message to %s", addr)
	} else {
		sb.packetQueue.PushBack(&udpPacket{Buf: synPacket, From: s})
	}
	sb.queueLock.Unlock()

	select {
	case sb.queueCh <- struct{}{}:
	default:
	}

	return nil
}

func (sb *Scuttlebutt) gossip() {
	defer metrics.MeasureSince([]string{"scuttlebutt", "gossip"}, time.Now())

	sb.mutex.Lock()

	liveNodesCount := sb.failureDetector.getLiveNodesCount()

	deadNodes := make([]string, 0)
	for _, node := range sb.failureDetector.getDeadNodes() {
		deadNodes = append(deadNodes, node.GossipPublicAddress)
	}

	seedNodes := make([]string, len(sb.clusterState.SeedNodes))
	copy(seedNodes, sb.clusterState.SeedNodes)

	var peerNodes []Node
	if sb.failureDetector.getLiveNodesCount() != 0 {
		peerNodes = sb.failureDetector.getLiveNodes()
	} else {
		peerNodes = sb.clusterState.getNodes()
	}

	sb.updateHeartbeat()
	sb.mutex.Unlock()

	nodes := getRandomNodes(sb.config.GossipNodes, peerNodes, sb.config.Node)

	hasGossipedSeedNode := false
	for _, node := range nodes {
		if slices.Contains(seedNodes, node.GossipPublicAddress) {
			hasGossipedSeedNode = true
			break
		}
	}

	var deadNode string
	selectionProbability := float64(len(deadNodes)) / float64(liveNodesCount+1)
	if selectionProbability > rand.Float64() {
		if len(deadNodes) > 0 {
			rand.Seed(time.Now().UnixNano())
			randoms := rand.Perm(len(deadNodes))
			deadNode = deadNodes[randoms[0]]
		}
	}

	var seedNode string
	if !hasGossipedSeedNode || liveNodesCount < len(seedNodes) {
		selectionProbability := float64(len(seedNodes)) / float64(liveNodesCount+len(deadNodes))
		if liveNodesCount == 0 || rand.Float64() <= selectionProbability {
			if len(seedNodes) > 0 {
				rand.Seed(time.Now().UnixNano())
				randoms := rand.Perm(len(seedNodes))
				seedNode = seedNodes[randoms[0]]
			}
		}
	}

	if len(nodes) > sb.config.GossipNodes {
		nodes = nodes[:sb.config.GossipNodes]
	}

	for _, node := range nodes {
		if err := sb.gossipOne(node.GossipPublicAddress); err != nil {
			sb.logger.Printf("[ERR] Gossip error with a live node %v\n", err)
		}
	}

	if deadNode != "" {
		if err := sb.gossipOne(deadNode); err != nil {
			sb.logger.Printf("[ERR] Gossip error with a dead node %v\n", err)
		}
	}

	if seedNode != "" {
		if err := sb.gossipOne(seedNode); err != nil {
			sb.logger.Printf("[ERR] Gossip error with a seed node %v\n", err)
		}
	}

	sb.mutex.Lock()
	defer sb.mutex.Unlock()
	sb.updateNodesState()
}

//Shutdown will stop all network activity
func (sb *Scuttlebutt) Shutdown() error {
	sb.shutdownLock.Lock()
	defer sb.shutdownLock.Unlock()

	if s := atomic.LoadInt32(&sb.shutdown); s == 1 {
		return nil
	}

	sb.net.shutdown()
	atomic.StoreInt32(&sb.shutdown, 1)
	close(sb.done)
	sb.wg.Wait()
	return nil
}

func (sb *Scuttlebutt) createSynPacket() ([]byte, error) {
	deadNodes := sb.failureDetector.getDeadNodes()
	digestMTU := udpTxBufferSize - 2 - len(sb.config.ClusterID) - 2 - sb.packetOverhead()
	digest := sb.clusterState.computeRandomDigest(digestMTU, deadNodes)
	buf, err := serializeSynPacket(sb.config.ClusterID, digest)
	if err != nil {
		return nil, err
	}

	if sb.config.EnableCompression {
		buf, err = compressSerializedPacket(buf)
	}

	if sb.encryptionEnabled {
		buf, err = encryptSerializedPacket(sb.config.SecretKey, buf)
	}
	return buf, err
}

func (sb *Scuttlebutt) computeDigest() (*digest, []Node) {
	deadNodes := sb.failureDetector.getDeadNodes()
	return sb.clusterState.computeRandomDigest(udpTxBufferSize/2, deadNodes), deadNodes
}

func (sb *Scuttlebutt) packetOverhead() int {
	size := 1
	if sb.config.EnableCompression {
		size++
	}
	if sb.encryptionEnabled {
		size += 1 + aes.BlockSize + aesNonceSize
	}
	return size
}

func (sb *Scuttlebutt) serializeAckPacket(digest *digest, delta *delta) ([]byte, error) {
	buf, err := serializeAckPacket(digest, delta)
	if sb.config.EnableCompression {
		buf, err = compressSerializedPacket(buf)
	}
	if sb.encryptionEnabled {
		buf, err = encryptSerializedPacket(sb.config.SecretKey, buf)
	}
	return buf, err
}

func (sb *Scuttlebutt) serializeAck2Packet(delta *delta) ([]byte, error) {
	buf, err := serializeAck2Packet(delta)
	if sb.config.EnableCompression {
		buf, err = compressSerializedPacket(buf)
	}
	if sb.encryptionEnabled {
		buf, err = encryptSerializedPacket(sb.config.SecretKey, buf)
	}
	return buf, err
}

func (sb *Scuttlebutt) deserializePacket(pktBuf []byte) (messageType, interface{}, error) {
	var err error

	msgType := messageType(pktBuf[0])
	if msgType == encryptedMsg {
		if !sb.encryptionEnabled {
			return msgType, nil, errRemotePacketEncryption
		}
		pktBuf, err = decryptSerializedPacket(sb.config.SecretKey, pktBuf[1:])
		if err != nil {
			return msgType, nil, err
		}
	}

	msgType, pkt, err := deserializePacket(pktBuf)
	if err != nil {
		return msgType, nil, err
	}

	return msgType, pkt, nil
}

func (sb *Scuttlebutt) processPacket(pktBuf []byte) (messageType, []byte, error) {
	msgType, pkt, err := sb.deserializePacket(pktBuf)
	if err != nil {
		return msgType, nil, err
	}

	switch msgType {
	case synMsg:
		msg := pkt.(synMessage)
		if msg.ClusterID != sb.config.ClusterID {
			buf, err := serializeWrongClusterPacket()
			return msgType, buf, err
		}

		digest, deadNodes := sb.computeDigest()
		deltaMTU := udpTxBufferSize - digest.serializedSize() - ackHeaderSize - sb.packetOverhead()
		delta := sb.clusterState.computeDelta(deltaMTU, msg.Digest, deadNodes)
		sb.reportToFailureDetector(delta)
		buf, err := sb.serializeAckPacket(digest, delta)
		return msgType, buf, err

	case ackMsg:
		msg := pkt.(ackMessage)
		sb.reportToFailureDetector(msg.Delta)
		nodeUpdates := sb.clusterState.applyDelta(msg.Delta)
		sb.handleNodeEventUpdate(nodeUpdates)
		deadNodes := sb.failureDetector.getDeadNodes()
		deltaMtu := udpTxBufferSize - sb.packetOverhead()
		delta := sb.clusterState.computeDelta(deltaMtu, msg.Digest, deadNodes)
		buf, err := sb.serializeAck2Packet(delta)
		return msgType, buf, err

	case ack2Msg:
		msg := pkt.(ack2Message)
		sb.reportToFailureDetector(msg.Delta)
		nodeUpdates := sb.clusterState.applyDelta(msg.Delta)
		sb.handleNodeEventUpdate(nodeUpdates)
		return msgType, nil, nil

	case wrongClusterMsg:
		sb.logger.Printf("[WARN] message rejected by peer: cluster ID mismatch")
		return msgType, nil, nil
	}
	return msgType, nil, fmt.Errorf("unknown message type")
}

func (sb *Scuttlebutt) reportToFailureDetector(delta *delta) {
	for node, nodedelta := range delta.NodeDeltas {
		nodestate := sb.clusterState.getNodeStateDefault(node)
		localMaxVersion := nodestate.getMaxVersion()
		if localMaxVersion < nodedelta.maxVersion() {
			sb.failureDetector.reportHeartbeat(node)
		}
	}
}

func (sb *Scuttlebutt) updateNodesState() {
	liveNodesBefore := sb.failureDetector.getLiveNodes()
	clusterNodes := sb.clusterState.getNodes()
	for _, node := range clusterNodes {
		if node != sb.config.Node {
			sb.failureDetector.updateNodeState(node)
		}
	}

	liveNodesAfter := sb.failureDetector.getLiveNodes()

	if !nodesMatch(liveNodesAfter, liveNodesBefore) {
		sb.handleNodeEventState(liveNodesBefore, liveNodesAfter)
	}

	evNodes := sb.failureDetector.evictDeadNodes()
	for _, node := range evNodes {
		sb.clusterState.removeNode(node)
	}
}

func (sb *Scuttlebutt) updateHeartbeat() {
	sb.heartbeat++
	hb := sb.heartbeat
	nodeState := sb.clusterState.getNodeStateDefault(sb.config.Node)
	nodeState.set(heartbeatKey, strconv.FormatUint(hb, 10))
}

func (sb *Scuttlebutt) stateSnapshot() *clusterStateSnapshot {
	return sb.clusterState.snapshot()
}

func (sb *Scuttlebutt) getSelfNodeState() *nodeState {
	return sb.clusterState.getNodeStateDefault(sb.config.Node)
}

func (sb *Scuttlebutt) handleNodeEventUpdate(nodeUpdates map[Node]struct{}) {
	if sb.config.Events != nil {
		sb.notifyLock.Lock()
		for k := range nodeUpdates {
			sb.notifyQueue.PushBack(&NodeEvent{Event: NodeUpdate, Node: k})
		}
		sb.notifyLock.Unlock()
		if len(nodeUpdates) > 0 {
			select {
			case sb.notifyCh <- struct{}{}:
			default:
			}
		}
	}
}

func (sb *Scuttlebutt) handleNodeEventState(before, after []Node) {
	if sb.config.Events != nil {
		sb.notifyLock.Lock()
		for _, node := range after {
			if !slices.Contains(before, node) {
				sb.notifyQueue.PushBack(&NodeEvent{Event: NodeLive, Node: node})
			}
		}
		for _, node := range before {
			if !slices.Contains(after, node) {
				sb.notifyQueue.PushBack(&NodeEvent{Event: NodeDead, Node: node})
			}
		}
		sb.notifyLock.Unlock()
		select {
		case sb.notifyCh <- struct{}{}:
		default:
		}
	}
}

//Join is used to join a cluster by contacting all given hosts
func (sb *Scuttlebutt) Join(seedNodes []string) error {
	sb.mutex.Lock()
	defer sb.mutex.Unlock()
	for _, node := range seedNodes {
		sb.clusterState.SeedNodes = append(sb.clusterState.SeedNodes, node)
	}
	return nil
}

//Set key-value metadata
func (sb *Scuttlebutt) Set(key, value string) error {
	sb.mutex.Lock()
	defer sb.mutex.Unlock()
	sb.getSelfNodeState().set(key, value)
	return nil
}

//State returns cluster state
func (sb *Scuttlebutt) State() *State {
	sb.mutex.Lock()
	defer sb.mutex.Unlock()

	state := sb.stateSnapshot()
	liveNodes := sb.failureDetector.getLiveNodes()
	deadNodes := sb.failureDetector.getDeadNodes()
	return &State{state: state, liveNodes: liveNodes, deadNodes: deadNodes}
}

//GetLiveNodes returns live nodes
func (s *State) GetLiveNodes() []Node {
	return s.liveNodes
}

//GetDeadNodes returns dead nodes
func (s *State) GetDeadNodes() []Node {
	return s.deadNodes
}

//GetAll returns node metadata
func (s *State) GetAll(node Node) map[string]string {
	keys := make(map[string]string, 0)
	if nodestate, ok := s.state.NodeStates[node]; ok {
		for k, v := range nodestate.KV {
			if k == heartbeatKey {
				continue
			}
			keys[k] = v.Value
		}
	}
	return keys
}

//SeedNodes returns seed nodes
func (s *State) SeedNodes() []string {
	return s.state.SeedNodes
}

//Get node-key value
func (s *State) Get(node Node, key string) (string, bool) {
	nodestate, ok := s.state.NodeStates[node]
	if ok {
		return nodestate.get(key)
	}
	return "", false
}
