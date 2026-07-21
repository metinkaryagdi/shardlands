package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// cluster: testler için N düğümlük in-memory Raft kümesi. Her düğümün
// state machine'i, uygulanan komutları string olarak biriktiren bir
// dilimdir.
type cluster struct {
	t        *testing.T
	nw       *Network
	ids      []string
	storages map[string]*MemoryStorage

	mu      sync.Mutex
	nodes   map[string]*Node
	applied map[string][]string
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		nw:       NewNetwork(),
		storages: map[string]*MemoryStorage{},
		nodes:    map[string]*Node{},
		applied:  map[string][]string{},
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.storages[id] = NewMemoryStorage()
		c.start(id)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) start(id string) {
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	node, err := NewNode(Config{
		ID:        id,
		Peers:     peers,
		Transport: c.nw.Transport(id),
		Storage:   c.storages[id],
		Apply: func(m ApplyMsg) {
			c.mu.Lock()
			c.applied[id] = append(c.applied[id], string(m.Cmd))
			c.mu.Unlock()
		},
		// Hızlı testler için sıkıştırılmış ama oranı korunmuş zamanlar.
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		TickInterval:       5 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatalf("NewNode(%s): %v", id, err)
	}
	c.mu.Lock()
	c.nodes[id] = node
	c.mu.Unlock()
	c.nw.Register(id, node)
}

func (c *cluster) node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// crash: düğümü durdurur ve ağdan koparır; Storage yerinde kalır.
func (c *cluster) crash(id string) {
	c.nw.Deregister(id)
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	n.Stop()
}

// restart: aynı Storage ile taze düğüm — state machine sıfırdan
// başlar (applied temizlenir; gerçekte snapshot devralırdı, o kapsam
// dışı) ama Raft HardState diskten gelir.
func (c *cluster) restart(id string) {
	c.mu.Lock()
	c.applied[id] = nil
	c.mu.Unlock()
	c.start(id)
}

func (c *cluster) appliedOf(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.applied[id]...)
}

// waitLeaderAmong: verilen kümede TAM BİR lider oturana kadar bekler.
func (c *cluster) waitLeaderAmong(ids []string, timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []string
		for _, id := range ids {
			if n := c.node(id); n != nil {
				if _, isLeader := n.Status(); isLeader {
					leaders = append(leaders, id)
				}
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v within %v", ids, timeout)
	return ""
}

func (c *cluster) waitLeader(timeout time.Duration) string {
	c.t.Helper()
	return c.waitLeaderAmong(c.ids, timeout)
}

// propose: id'deki düğüme önerir; lider değilse test düşer.
func (c *cluster) propose(id, cmd string) {
	c.t.Helper()
	if _, _, ok := c.node(id).Propose([]byte(cmd)); !ok {
		c.t.Fatalf("%s rejected proposal %q (not leader)", id, cmd)
	}
}

// waitAppliedEqual: verilen düğümlerin state machine'leri tam olarak
// want dizisine yakınsayana kadar bekler.
func (c *cluster) waitAppliedEqual(ids []string, want []string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for _, id := range ids {
			if fmt.Sprint(c.appliedOf(id)) != fmt.Sprint(want) {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		c.t.Logf("%s applied: %v", id, c.appliedOf(id))
	}
	c.t.Fatalf("logs did not converge to %v within %v", want, timeout)
}
