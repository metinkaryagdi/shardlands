// Package matchmaking, oyuncuları eşleştirir ve maç kurulumunu SAGA ile
// yürütür.
//
// Atomiklik problemi: "arena instance'ı oluştur + oyuncuları ata" ya
// tamamen olmalı ya hiç. Yarıda kalırsa ya boş arena sızar (kaynak
// israfı) ya oyuncular kuyrukta kaybolur. Dağıtık transaction yok;
// çözüm yine saga (bkz. services/trade) — adımlar + telafiler:
//
//	provision başarısız → oyuncular kuyruğa iade
//	atama başarısız     → atananlar geri alınır, arena yıkılır, kuyruğa iade
//
// Saga'nın kendi event log'u (match-<id>) hem durumu hem denetim izidir.
package matchmaking

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
)

// Service, matchmaking gRPC uçtur; işi Matcher'a devreder.
type Service struct {
	pb.UnimplementedMatchmakingServiceServer
	m *Matcher
}

func New(m *Matcher) *Service { return &Service{m: m} }

// Enqueue, oyuncuyu kuyruğa alır ve sırasını döner. Yeterli oyuncu
// birikince maç saga'sı arka planda başlar.
func (s *Service) Enqueue(_ context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	if req.GetPlayerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "player_id is required")
	}
	pos, err := s.m.Enqueue(req.GetPlayerId(), req.GetMode())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &pb.EnqueueResponse{Position: int32(pos)}, nil
}
