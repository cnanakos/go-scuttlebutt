package scuttlebutt

//NodeEventType are the types that can be sent over the ChannelEvent
type NodeEventType int

const (
	NodeLive NodeEventType = iota
	NodeDead
	NodeUpdate
)

//NotifyEvent is used to receive notifications about nodes joining, updating
//their metadata and leaving the cluster.
type NotifyEvent interface {
	//NotifyLive is invoked when a node becomes live
	NotifyLive(Node)

	//NotifyDead is invoked when a node becomes dead
	NotifyDead(Node)

	//NotifyUpdate is invoked when a node updates it's meta
	NotifyUpdate(Node)
}

//NodeEvent is an event related to node activity
type NodeEvent struct {
	Event NodeEventType
	Node  Node
}

var _ NotifyEvent = (*ChannelEvent)(nil)

//ChannelEvent is used to receive events, this channel should process events without
//blocking in a timely manner.
type ChannelEvent struct {
	Ch chan<- NodeEvent
}

func (c *ChannelEvent) NotifyLive(n Node) {
	c.Ch <- NodeEvent{NodeLive, n}
}

func (c *ChannelEvent) NotifyDead(n Node) {
	c.Ch <- NodeEvent{NodeDead, n}
}

func (c *ChannelEvent) NotifyUpdate(n Node) {
	c.Ch <- NodeEvent{NodeUpdate, n}
}
