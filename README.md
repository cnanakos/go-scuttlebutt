scuttlebutt is a [Go](http://www.golang.org) library for distributed cluster
membership and failure detection via an efficient reconciliation and
flow-control anti-entropy gossip protocol.

The failure detection mechanism is based on the phi accrual failure detector
used to mark failing nodes and remove them from the membership.

## Usage
```go
// Create scuttlebutt instance with a default LAN configuration
sb, err := scuttlebutt.NewScuttlebutt(scuttlebutt.DefaultLANConfig(), map[string]string{"key1": "value1"})
if err != nil {
    panic("Failed to create scuttlebutt instance: " + err.Error())
}

//Join cluster via a seed node
err = sb.Join([]string{"1.2.3.5:9111"})
if err != nil {
    panic("Failed to join cluster: " + err.Error())
}

// Get cluster state and check live and dead nodes
for _, node := range sb.State().GetLiveNodes() {
    fmt.Printf("Live Node: %s %s\n", node.ID, node.GossipPublicAddress)
}

for _, node := range sb.State().GetDeadNodes() {
    fmt.Printf("Dead Node: %s %s\n", node.ID, node.GossipPublicAddress)
}
// scuttlebutt will maintain node membership in the background. Events can be
// used as notifications when members join, leave or update their metadata state.
```

Failure detector can be configured in order to tune state propagation and
convergence times. Higher convergence has a cost of higher bandwidth usage.

For complete documentation, see the associated [Godoc](http://godoc.org/github.com/cnanakos/go-scuttlebutt).

# References

- ScuttleButt paper: https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
- Phi Accrual error detection: https://www.researchgate.net/publication/29682135_The_ph_accrual_failure_detector
