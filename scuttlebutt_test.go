package scuttlebutt

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testListenAddr string = "127.0.0.1:10000"
)

type sbTestNodes struct {
	sb []*Scuttlebutt
	ch []chan NodeEvent
}

func yield() {
	time.Sleep(1000 * time.Millisecond)
}

func createLocalhostConfig(port int, ch chan NodeEvent) Config {
	config := Config{
		Node: Node{ID: getTestUUID(),
			GossipPublicAddress: fmt.Sprintf("localhost:%d", port)},
		ClusterID:             "default-test-cluster",
		GossipNodes:           3,
		GossipInterval:        100 * time.Millisecond,
		ListenAddr:            fmt.Sprintf("localhost:%d", port),
		FailureDetectorConfig: FailureDetectorDefaultConfig(),
		EnableCompression:     true,
	}
	config.FailureDetectorConfig.DeadNodeGracePeriod = 2 * time.Second
	if ch != nil {
		config.Events = &ChannelEvent{Ch: ch}
	}
	return config
}

func createScuttlebuttNodes(t *testing.T, nr int, encrypted bool) *sbTestNodes {
	nodes := &sbTestNodes{
		sb: make([]*Scuttlebutt, 0),
		ch: make([]chan NodeEvent, 0),
	}

	var key []byte
	if encrypted {
		key = make([]byte, 32)
		_, err := rand.Read(key)
		require.Nil(t, err)
	}

	for i := 0; i < nr; i++ {
		var config Config
		if i == 0 {
			eventCh := make(chan NodeEvent, nr+1)
			nodes.ch = append(nodes.ch, eventCh)
			config = createLocalhostConfig(19111+i, eventCh)
			config.SecretKey = key
			sb, err := NewScuttlebutt(&config, nil)
			require.Nil(t, err)
			nodes.sb = append(nodes.sb, sb)
		} else {
			config = createLocalhostConfig(19111+i, nil)
			config.SecretKey = key
			sb, err := NewScuttlebutt(&config, nil)
			require.Nil(t, err)
			err = sb.Join([]string{nodes.sb[0].config.ListenAddr})
			require.Nil(t, err)
			nodes.sb = append(nodes.sb, sb)
		}
	}
	return nodes
}

func addScuttlebuttNode(t *testing.T, nodes *sbTestNodes, encrypted bool) {
	s := strings.Split(nodes.sb[len(nodes.sb)-1].config.ListenAddr, ":")
	maxPort, _ := strconv.Atoi(s[1])
	config := createLocalhostConfig(maxPort+1, nil)
	if encrypted {
		config.SecretKey = nodes.sb[0].config.SecretKey
	}
	sb, err := NewScuttlebutt(&config, nil)
	require.Nil(t, err)
	err = sb.Join([]string{nodes.sb[0].config.ListenAddr})
	require.Nil(t, err)
	nodes.sb = append(nodes.sb, sb)
}

func waitEventNodeGossip(t *testing.T, ch chan NodeEvent, expectedNodes []Node, nodeEvent NodeEventType) {
	nodes := make([]Node, 0)
	for i := 0; i < len(expectedNodes); i++ {
		event := <-ch
		require.Equal(t, nodeEvent, event.Event)
		nodes = append(nodes, event.Node)
	}
	require.ElementsMatch(t, expectedNodes, nodes)
}

func runScuttlebuttHandshake(t *testing.T, sb1 *Scuttlebutt, sb2 *Scuttlebutt) {
	synPacket, err := sb1.createSynPacket()
	require.Nil(t, err)

	_, ackPacket, err := sb2.processPacket(synPacket)
	require.Nil(t, err)

	_, ack2Packet, err := sb1.processPacket(ackPacket)
	require.Nil(t, err)

	_, emptyPacket, err := sb2.processPacket(ack2Packet)
	require.Nil(t, emptyPacket)
	require.Nil(t, err)

	require.Equal(t, len(sb1.clusterState.NodeStates), len(sb2.clusterState.NodeStates))
	for node, nodestate := range sb1.clusterState.NodeStates {
		peerNodeState := sb2.clusterState.NodeStates[node]
		for key, value := range nodestate.KV {
			if key == heartbeatKey {
				continue
			}
			require.Equal(t, value, peerNodeState.KV[key])
		}
	}
}

func TestScuttlebuttHandshakeEmptyKeysNoSeed(t *testing.T) {
	config1 := createLocalhostConfig(9111, nil)
	config2 := createLocalhostConfig(9112, nil)

	sb1, err := NewScuttlebutt(&config1, nil)
	require.Nil(t, err)
	sb2, err := NewScuttlebutt(&config2, nil)
	require.Nil(t, err)
	runScuttlebuttHandshake(t, sb1, sb2)
	sb1.Shutdown()
	sb2.Shutdown()
}

func TestScuttlebuttHandshakeWithKeysNoSeed(t *testing.T) {
	config1 := createLocalhostConfig(9111, nil)
	config2 := createLocalhostConfig(9112, nil)

	sb1, err := NewScuttlebutt(&config1, map[string]string{"key1a": "1", "key2a": "2"})
	require.Nil(t, err)
	sb2, err := NewScuttlebutt(&config2, map[string]string{"key1b": "1", "key2b": "2"})
	require.Nil(t, err)
	runScuttlebuttHandshake(t, sb1, sb2)
	//no state change
	runScuttlebuttHandshake(t, sb1, sb2)
	{
		state1 := sb1.getSelfNodeState()
		state1.set("key1a", "3")
		state1.set("key1c", "4")
	}
	runScuttlebuttHandshake(t, sb1, sb2)
	sb1.Shutdown()
	sb2.Shutdown()
}

func TestScuttlebuttMultipleNodes(t *testing.T) {
	nodes := createScuttlebuttNodes(t, 5, false)
	expected := make([]Node, 0)
	for i := 1; i < 5; i++ {
		expected = append(expected, nodes.sb[i].config.Node)
	}
	waitEventNodeGossip(t, nodes.ch[0], expected, NodeLive)
	for _, sb := range nodes.sb {
		sb.Shutdown()
	}
}

func TestScuttlebuttLiveDownLiveEvictDeadNodes(t *testing.T) {
	testSBLiveDownLiveEvictDeadNodes(t, false)
}

func TestScuttlebuttLiveDownLiveEvictDeadNodesEncrypted(t *testing.T) {
	testSBLiveDownLiveEvictDeadNodes(t, true)
}

func testSBLiveDownLiveEvictDeadNodes(t *testing.T, encrypted bool) {
	nodes := createScuttlebuttNodes(t, 5, encrypted)
	defer func() {
		for _, sb := range nodes.sb {
			sb.Shutdown()
		}
	}()

	waitEventNodeGossip(t, nodes.ch[0], []Node{
		nodes.sb[1].config.Node,
		nodes.sb[2].config.Node,
		nodes.sb[3].config.Node,
		nodes.sb[4].config.Node,
	}, NodeLive)

	nodes.sb[2].Shutdown()

	waitEventNodeGossip(t, nodes.ch[0], []Node{
		nodes.sb[2].config.Node,
	}, NodeDead)

	require.Equal(t, []Node{nodes.sb[2].config.Node}, nodes.sb[0].State().GetDeadNodes())
	time.Sleep(3 * time.Second)
	require.Equal(t, []Node{}, nodes.sb[0].State().GetDeadNodes())

	addScuttlebuttNode(t, nodes, encrypted)

	waitEventNodeGossip(t, nodes.ch[0], []Node{
		nodes.sb[5].config.Node,
	}, NodeLive)
}
