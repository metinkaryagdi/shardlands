// Package matchmaking, Faz 1'de yalnızca kuyruk iskeletidir: oyuncular
// moda göre sıraya girer, sıra numarası döner. Maç oluşturma (arena
// provision + oyuncu atama atomikliği, saga ile) Faz 5'in konusu.
package matchmaking

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
)

var validModes = map[string]bool{"1v1": true, "2v2": true}

type Service struct {
	pb.UnimplementedMatchmakingServiceServer

	mu     sync.Mutex
	queues map[string][]string // mode → sıradaki player id'ler
}

func New() *Service {
	return &Service{queues: map[string][]string{}}
}

func (s *Service) Enqueue(_ context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	if req.GetPlayerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "player_id is required")
	}
	if !validModes[req.GetMode()] {
		return nil, status.Errorf(codes.InvalidArgument, "unknown mode %q", req.GetMode())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[req.GetMode()]
	// İdempotent: zaten kuyruktaysa mevcut sırasını dön (istemci retry
	// atabilir — Faz 0 Raft churn testinin öğrettiği ders).
	for i, id := range q {
		if id == req.GetPlayerId() {
			return &pb.EnqueueResponse{Position: int32(i + 1)}, nil
		}
	}
	s.queues[req.GetMode()] = append(q, req.GetPlayerId())
	return &pb.EnqueueResponse{Position: int32(len(q) + 1)}, nil
}
