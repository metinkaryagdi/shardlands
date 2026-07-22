// Arena Pod sunucusu.
//
// Operator'ün ürettiği Pod bu binary'yi çalıştırır ve yapılandırmayı
// ortam değişkenlerinden okur (operator/controller/arena_controller.go
// ile aynı sözleşme):
//
//	ARENA_ID       arena kimliği
//	ARENA_MODE     "1v1" | "2v2"
//	ARENA_PLAYERS  "p1:0,p2:1"  (playerID:team virgülle)
//
// Maç bitince süreç 0 ile çıkar; Pod Succeeded olur ve operator
// temizler (RestartPolicy=Never).
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/services/arena"
)

func main() {
	id := envOr("ARENA_ID", "arena-local")
	mode := arena.Mode(envOr("ARENA_MODE", string(arena.Mode1v1)))
	addr := ":" + envOr("ARENA_PORT", "7777")

	specs, err := parsePlayers(os.Getenv("ARENA_PLAYERS"))
	if err != nil {
		log.Fatalf("arena: ARENA_PLAYERS: %v", err)
	}

	done := make(chan arena.Result, 1)
	a := arena.New(id, mode, specs, arena.Options{
		OnEnd: func(r arena.Result) { done <- r },
	})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("arena: listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterArenaServiceServer(gs, arena.NewServer(a))
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("arena: serve: %v", err)
		}
	}()

	log.Printf("arena %s (%s) %s üzerinde, %d oyuncu", id, mode, addr, len(specs))
	a.Run()

	res := <-done
	log.Printf("arena %s bitti: kazanan takım %d, %d tick", res.ArenaID, res.WinnerTeam, res.Ticks)
	gs.GracefulStop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parsePlayers, "p1:0,p2:1" biçimini çözer.
func parsePlayers(s string) ([]arena.PlayerSpec, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, arena.ErrNoPlayers
	}
	var out []arena.PlayerSpec
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, teamStr, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("geçersiz oyuncu %q (beklenen id:team)", part)
		}
		team, err := strconv.Atoi(teamStr)
		if err != nil || team < 0 || team > 1 {
			return nil, fmt.Errorf("geçersiz takım %q", teamStr)
		}
		out = append(out, arena.PlayerSpec{ID: id, Name: id, Team: team})
	}
	if len(out) == 0 {
		return nil, arena.ErrNoPlayers
	}
	return out, nil
}
