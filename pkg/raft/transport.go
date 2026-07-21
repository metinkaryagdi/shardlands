package raft

import (
	"errors"
	"sync"
)

// Transport, RPC katmanının soyutlamasıdır. Node yalnızca bunu görür;
// altında in-memory Network (test/chaos) veya gRPC (Faz 3) olabilir.
// Çağrılar senkron ve bloklamalıdır; hata "ulaşılamadı" demektir —
// Raft zaten mesaj kaybına dayanıklıdır, retry üst katmanın (heartbeat
// döngüsünün) işidir.
type Transport interface {
	RequestVote(target string, req *RequestVoteReq) (*RequestVoteResp, error)
	AppendEntries(target string, req *AppendEntriesReq) (*AppendEntriesResp, error)
}

// ErrUnreachable: hedef bölünmüş, çökmüş veya kayıtsız.
var ErrUnreachable = errors.New("raft: peer unreachable")

// Network, partition simüle edebilen in-memory RPC dokusudur. Her
// düğüm bir gruba aittir; yalnızca AYNI gruptakiler konuşabilir.
// Böylece "ağ bölünmesi" bir test aracı değil, birinci sınıf senaryo
// olur — Raft'ın bütün ilginç davranışları bölünme sırasında yaşanır.
type Network struct {
	mu    sync.Mutex
	nodes map[string]*Node
	group map[string]int
}

func NewNetwork() *Network {
	return &Network{nodes: map[string]*Node{}, group: map[string]int{}}
}

// Register, düğümü dokuya bağlar (varsayılan grup 0).
func (nw *Network) Register(id string, n *Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = n
	if _, ok := nw.group[id]; !ok {
		nw.group[id] = 0
	}
}

// Deregister, düğümü dokudan koparır (crash simülasyonu).
func (nw *Network) Deregister(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.nodes, id)
}

// Partition, düğümleri ayrık gruplara böler: groups[i] içindekiler
// grup i+1'e taşınır, listelenmeyenler grup 0'da kalır.
func (nw *Network) Partition(groups ...[]string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for id := range nw.group {
		nw.group[id] = 0
	}
	for i, g := range groups {
		for _, id := range g {
			nw.group[id] = i + 1
		}
	}
}

// Isolate, verilen düğümleri tek başına bir gruba alır.
func (nw *Network) Isolate(ids ...string) { nw.Partition(ids) }

// Heal, tüm bölünmeleri kaldırır.
func (nw *Network) Heal() { nw.Partition() }

// Transport, verilen düğüm için doku üzerinden konuşan uç döner.
func (nw *Network) Transport(id string) Transport {
	return &netTransport{nw: nw, from: id}
}

func (nw *Network) route(from, to string) (*Node, error) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	n, ok := nw.nodes[to]
	if !ok || nw.group[from] != nw.group[to] {
		return nil, ErrUnreachable
	}
	return n, nil
}

type netTransport struct {
	nw   *Network
	from string
}

func (t *netTransport) RequestVote(target string, req *RequestVoteReq) (*RequestVoteResp, error) {
	n, err := t.nw.route(t.from, target)
	if err != nil {
		return nil, err
	}
	return n.HandleRequestVote(req)
}

func (t *netTransport) AppendEntries(target string, req *AppendEntriesReq) (*AppendEntriesResp, error) {
	n, err := t.nw.route(t.from, target)
	if err != nil {
		return nil, err
	}
	return n.HandleAppendEntries(req)
}
