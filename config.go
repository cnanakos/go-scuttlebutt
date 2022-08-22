package scuttlebutt

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Config struct {
	//ID of the cluster this node is part.
	ClusterID string
	//Unique node definition.
	Node Node
	//GossipNodes is the number of random nodes gossiping per GossipInterval
	GossipNodes int
	//Interval between sending gossip messages.
	GossipInterval time.Duration
	//Address and port to listen on.
	ListenAddr string
	//See FailureDetectorConfig.
	FailureDetectorConfig *FailureDetectorConfig
	//Define a custom logger. If Logger is not set it will use os.Stderr
	//instead.
	Logger *log.Logger
	//Events for receiving notifications via callback mechanisms. For Events,
	//see the NotifyEvent interface.
	Events NotifyEvent
	//Control message compression.
	EnableCompression bool
	//Enable message-level encryption. The value should be either 16, 24, or
	//32 bytes to select AES-128, AES-192, or AES-256.
	SecretKey []byte
}

//DefaultLANConfig returns a configuration suitable for most LAN
//environments. It creates a unique Node ID based on the hostname of the node
//and a timestamp. It listens to all interfaces on port 9111 and the ClusterID
//used is "default".
func DefaultLANConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		ClusterID: "default",
		Node: Node{ID: fmt.Sprintf("%s/%d", hostname, time.Now().UnixNano()),
			GossipPublicAddress: "0.0.0.0:9111"},
		GossipNodes:           3,
		GossipInterval:        1000 * time.Millisecond,
		ListenAddr:            "0.0.0.0:9111",
		FailureDetectorConfig: FailureDetectorDefaultConfig(),
		Logger:                nil,
		Events:                nil,
		EnableCompression:     true,
		SecretKey:             nil,
	}
}

//DefaultWANConfig returns a configuration suitable for WAN environments based
//on the DefaultLANConfig.
func DefaultWANConfig() *Config {
	conf := DefaultLANConfig()
	conf.GossipNodes = 4
	conf.GossipInterval = 3000 * time.Millisecond
	conf.FailureDetectorConfig.PhiThreshold = 12
	conf.FailureDetectorConfig.AcceptableHeartbeatPause = 10 * time.Second
	return conf
}
