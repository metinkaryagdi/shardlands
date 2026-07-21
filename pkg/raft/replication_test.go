package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Temel replikasyon: liderin kabul ettiği komutlar tüm düğümlerde aynı
// sırayla uygulanmalı.
func TestReplicateBasic(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(3 * time.Second)

	want := []string{"a", "b", "c", "d", "e"}
	for _, cmd := range want {
		c.propose(leader, cmd)
	}
	c.waitAppliedEqual(c.ids, want, 3*time.Second)
}

// Partition testi: izole follower, bölünme sırasında yazılanları
// iyileşince eksiksiz ve aynı sırada almalı.
func TestFollowerCatchUpAfterPartition(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(3 * time.Second)

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.nw.Isolate(follower)

	want := []string{"w1", "w2", "w3"}
	for _, cmd := range want {
		c.propose(leader, cmd)
	}
	// Çoğunluk (2/3) izole follower olmadan da commit edebilmeli.
	var majority []string
	for _, id := range c.ids {
		if id != follower {
			majority = append(majority, id)
		}
	}
	c.waitAppliedEqual(majority, want, 3*time.Second)

	c.nw.Heal()
	c.waitAppliedEqual(c.ids, want, 3*time.Second) // follower yakaladı
}

// Raft'ın ana güvenlik senaryosu: azınlıkta kalan lider yazı kabul
// edebilir ama COMMIT EDEMEZ; çoğunluk tarafın yazdıkları kazanır,
// azınlığın kayıtları iyileşmede silinir ve hiçbir yerde uygulanmaz.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	oldLeader := c.waitLeader(5 * time.Second)

	// Eski lider + 1 follower azınlıkta; 3 düğüm çoğunlukta.
	var minority, majority []string
	minority = append(minority, oldLeader)
	for _, id := range c.ids {
		if id == oldLeader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.nw.Partition(minority, majority)

	// Azınlık lideri hâlâ lider sanır ve öneriyi kabul eder...
	if _, _, ok := c.node(oldLeader).Propose([]byte("lost")); !ok {
		t.Fatal("old leader should still accept proposals (it cannot know yet)")
	}
	// ...ama commit çoğunluk ister: hiçbir düğümde uygulanmamalı.
	time.Sleep(300 * time.Millisecond)
	for _, id := range c.ids {
		for _, cmd := range c.appliedOf(id) {
			if cmd == "lost" {
				t.Fatalf("%s applied a command that never had quorum", id)
			}
		}
	}

	// Çoğunluk taraf yeni lider seçer ve gerçek yazılar oradan akar.
	newLeader := c.waitLeaderAmong(majority, 5*time.Second)
	want := []string{"won-1", "won-2", "won-3"}
	for _, cmd := range want {
		c.propose(newLeader, cmd)
	}
	c.waitAppliedEqual(majority, want, 3*time.Second)

	// İyileşme: eski lider çekilir, "lost" kaydı kesilir, herkes
	// çoğunluğun tarihine yakınsar.
	c.nw.Heal()
	c.waitAppliedEqual(c.ids, want, 5*time.Second)
}

// Crash + restart: HardState (dönem/oy/log) diskten geri gelmeli,
// düğüm kümeye yetişmeli.
func TestRestartRejoinsAndCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(3 * time.Second)

	c.propose(leader, "before-1")
	c.propose(leader, "before-2")
	c.waitAppliedEqual(c.ids, []string{"before-1", "before-2"}, 3*time.Second)

	var victim string
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}
	c.crash(victim)

	c.propose(leader, "during-1")
	c.propose(leader, "during-2")

	c.restart(victim)
	// Restart eden düğüm state machine'i sıfırdan kurar (snapshot yok)
	// ve TÜM log'u baştan uygular — kalıcı log'un kanıtı da bu.
	want := []string{"before-1", "before-2", "during-1", "during-2"}
	c.waitAppliedEqual(c.ids, want, 5*time.Second)
}

// Chaos: rastgele bölünme/iyileşme fırtınası altında öneriler akar;
// sonunda ağ iyileşince BÜTÜN düğümler AYNI diziye yakınsamalı ve bu
// dizi, commit sırasına göre kayıpsız/çiftsiz olmalı. (Hangi önerilerin
// commit olduğu belirsizdir — Raft'ın garantisi "kabul edilen her şey"
// değil, "commit edilenlerde mutabakat"tır.)
func TestChurnConvergence(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(5 * time.Second)
	rng := rand.New(rand.NewSource(7))

	stopChurn := time.After(2 * time.Second)
	seq := 0
churn:
	for {
		select {
		case <-stopChurn:
			break churn
		default:
		}
		// Rastgele iki gruba böl ya da iyileştir.
		if rng.Intn(3) == 0 {
			c.nw.Heal()
		} else {
			k := 1 + rng.Intn(2) // 1 veya 2 düğüm koparsın (çoğunluk yaşasın)
			perm := rng.Perm(len(c.ids))
			var cut []string
			for _, i := range perm[:k] {
				cut = append(cut, c.ids[i])
			}
			c.nw.Partition(cut)
		}
		// Kim lider olduğunu iddia ediyorsa ona öner (başarısızlık normal).
		for _, id := range c.ids {
			if _, isLeader := c.node(id).Status(); isLeader {
				seq++
				c.node(id).Propose([]byte(fmt.Sprintf("cmd-%d", seq)))
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.nw.Heal()

	// Tek Propose commit GARANTİSİ değildir: heal'in hemen ardından
	// "lider benim" diyen düğüm, henüz devrilmemiş bayat azınlık lideri
	// olabilir ve kabul ettiği işaret kesilip yok olur. Bu yüzden
	// benzersiz işaretlerle, biri her yerde uygulanana kadar yeniden
	// öneriyoruz (6.824'ün one() yardımcısıyla aynı desen).
	deadline := time.Now().Add(10 * time.Second)
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		marker := fmt.Sprintf("final-%d", attempt)
		proposed := false
		for _, id := range c.ids {
			if _, _, ok := c.node(id).Propose([]byte(marker)); ok {
				proposed = true
				break
			}
		}
		if !proposed {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// Bu işaret commit olduysa ondan önceki her şey de commit'tir
		// (log prefix garantisi): herkes aynı, işaretle biten diziye
		// yakınsamış olmalı.
		settle := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(settle) {
			ref := c.appliedOf(c.ids[0])
			if len(ref) > 0 && ref[len(ref)-1] == marker {
				same := true
				for _, id := range c.ids[1:] {
					if fmt.Sprint(c.appliedOf(id)) != fmt.Sprint(ref) {
						same = false
						break
					}
				}
				if same {
					// Çift uygulama kontrolü: her komut en fazla bir kez
					// (tüm önerilen komutlar benzersiz üretildi).
					seen := map[string]bool{}
					for _, cmd := range ref {
						if seen[cmd] {
							t.Fatalf("command %q applied twice", cmd)
						}
						seen[cmd] = true
					}
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, id := range c.ids {
		t.Logf("%s applied (%d): %v", id, len(c.appliedOf(id)), c.appliedOf(id))
	}
	t.Fatal("cluster did not converge after churn")
}

// AppendEntries çakışma birim testi: follower'daki çelişen kuyruk
// kesilip liderin kayıtlarıyla değiştirilmeli; örtüşen aynı kayıtlar
// (idempotentlik) dokunulmadan kalmalı.
func TestAppendEntriesTruncatesConflict(t *testing.T) {
	nw := NewNetwork()
	n, err := NewNode(Config{
		ID: "solo", Peers: nil, Transport: nw.Transport("solo"),
		ElectionTimeoutMin: time.Hour, ElectionTimeoutMax: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	n.mu.Lock()
	n.term = 4
	// Yerel log: [t1, t2, t2] — son iki kayıt eski bir liderin artığı.
	n.log = []Entry{{Term: 1, Cmd: []byte("a")}, {Term: 2, Cmd: []byte("x")}, {Term: 2, Cmd: []byte("y")}}
	n.mu.Unlock()

	// Lider (dönem 4): prev=(1,t1), sonrası [t3,t4] — index 2'de çakışma.
	resp, err := n.HandleAppendEntries(&AppendEntriesReq{
		Term: 4, LeaderID: "L", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:      []Entry{{Term: 3, Cmd: []byte("b")}, {Term: 4, Cmd: []byte("c")}},
		LeaderCommit: 0,
	})
	if err != nil || !resp.Success {
		t.Fatalf("append = %+v, %v; want success", resp, err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 3 || n.log[1].Term != 3 || n.log[2].Term != 4 || string(n.log[0].Cmd) != "a" {
		t.Fatalf("log after conflict = %+v, want [t1:a t3:b t4:c]", n.log)
	}
}
