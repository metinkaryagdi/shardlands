package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"shardlands/services/world"
)

// startReader, WS'i arka planda sürekli okuyup mesajları kanala akıtır.
// Neden goroutine? Canlı bağlantıya okuma deadline'ı koyup timeout almak
// gorilla'da bağlantıyı okunamaz hâle getirir; "mesaj GELMEMELİ"yi
// kanalın boş kalmasıyla ölçmek bağlantıyı bozmadan aynı şeyi söyler.
func startReader(ws *websocket.Conn) <-chan wireMsg {
	ch := make(chan wireMsg, 512)
	go func() {
		defer close(ch)
		for {
			ws.SetReadDeadline(time.Now().Add(30 * time.Second))
			var m wireMsg
			if err := ws.ReadJSON(&m); err != nil {
				return
			}
			select {
			case ch <- m:
			default: // tampon dolu: düşür (testin ilgilendiği akışın varlığı)
			}
		}
	}()
	return ch
}

// nextSnapshot, d içinde bir snapshot bekler.
func nextSnapshot(ch <-chan wireMsg, d time.Duration) (wireMsg, bool) {
	timeout := time.After(d)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return wireMsg{}, false
			}
			if m.Type == "snapshot" {
				return m, true
			}
		case <-timeout:
			return wireMsg{}, false
		}
	}
}

func waitShardAvailable(t *testing.T, srv *Server, shardID string, want bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if srv.Shards().Available(shardID) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("shard %s available=%v, want %v", shardID, srv.Shards().Available(shardID), want)
}

// CAP/PACELC deneyi (tam yığın):
//
//	Bir shard'ın Raft grubunda çoğunluk kaybolunca (bilinçli izolasyon) o
//	shard'ın bölgeleri DONAR — snapshot akmaz, komut işlenmez: tutarlılık
//	için kullanılabilirlikten vazgeçilir (CP). Buna karşılık CRDT tabanlı
//	global sayaç (/api/stats) hizmet vermeye devam eder — anlaşma
//	gerektirmeyen veri bölünmede de okunur (AP). İyileşince shard yeniden
//	lider seçer ve simülasyon kaldığı yerden sürer.
func TestCAPShardIsolationFreezesRegion(t *testing.T) {
	srv := startTestServer(t)
	id, tok := login(t, srv.HTTPAddr, "izole")
	ws := dialWS(t, srv.HTTPAddr, tok)
	ch := startReader(ws)

	snap, ok := nextSnapshot(ch, 3*time.Second)
	if !ok {
		t.Fatal("no initial snapshot")
	}
	regionID := snap.Region
	shardID := srv.Router().ShardOf(regionID)
	if regionID == "" || shardID == "" {
		t.Fatalf("region/shard missing: %q/%q", regionID, shardID)
	}
	t.Logf("oyuncu %s bölge=%s shard=%s", id, regionID, shardID)

	// --- BÖLÜNME: shard'ın tüm replikalarını ayır (hiçbir yerde çoğunluk yok)
	srv.Shards().Group(shardID).IsolateAll()
	waitShardAvailable(t, srv, shardID, false, 3*time.Second)

	// Yolda olan kareler tükensin.
	for {
		if _, more := nextSnapshot(ch, 400*time.Millisecond); !more {
			break
		}
	}
	// CP: artık yeni kare GELMEMELİ (bölge donuk).
	if _, got := nextSnapshot(ch, 800*time.Millisecond); got {
		t.Fatal("region kept ticking without quorum (CAP: must freeze)")
	}

	// AP: CRDT sayaç hâlâ okunuyor.
	resp, err := http.Get("http://" + srv.HTTPAddr + "/api/stats")
	if err != nil {
		t.Fatalf("stats during partition must still serve: %v", err)
	}
	var st struct {
		TotalGathered float64 `json:"totalGathered"`
		Online        float64 `json:"online"`
	}
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st.Online < 1 {
		t.Fatalf("online gauge = %v during partition, want >= 1", st.Online)
	}

	// --- İYİLEŞME
	srv.Shards().Group(shardID).Heal()
	waitShardAvailable(t, srv, shardID, true, 5*time.Second)
	if _, got := nextSnapshot(ch, 3*time.Second); !got {
		t.Fatal("snapshots did not resume after heal")
	}
}

// Bir shard'ın izolasyonu DİĞER shard'ın bölgelerini etkilememeli:
// sharding, arıza yarıçapını (blast radius) sınırlar.
func TestCAPIsolationBlastRadiusLimited(t *testing.T) {
	srv := startTestServer(t)

	byShard := map[string][]string{}
	for col := 0; col < world.Cols; col++ {
		for row := 0; row < world.Rows; row++ {
			rid := world.RegionAt(float64(col)*world.RegionW+1, float64(row)*world.RegionH+1)
			s := srv.Router().ShardOf(rid)
			byShard[s] = append(byShard[s], rid)
		}
	}
	if len(byShard) < 2 {
		t.Skip("bu kurulumda tüm bölgeler tek shard'ta")
	}

	var downShard, upShard string
	for s := range byShard {
		if downShard == "" {
			downShard = s
		} else if upShard == "" {
			upShard = s
		}
	}

	srv.Shards().Group(downShard).IsolateAll()
	waitShardAvailable(t, srv, downShard, false, 3*time.Second)
	if !srv.Shards().Available(upShard) {
		t.Fatalf("%s became unavailable due to %s isolation (blast radius leak)", upShard, downShard)
	}
	for _, rid := range byShard[downShard] {
		if srv.Router().ShardUp(srv.Router().ShardOf(rid)) {
			t.Fatalf("region %s reports up while its shard is isolated", rid)
		}
	}
	for _, rid := range byShard[upShard] {
		if !srv.Router().ShardUp(srv.Router().ShardOf(rid)) {
			t.Fatalf("region %s reports down though its shard is healthy", rid)
		}
	}
}
