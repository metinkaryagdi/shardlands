// Package player, kimlik ve oyuncu kayıtlarının sahibidir. Faz 1'de
// in-memory bir gRPC servisi; Faz 2'de event-sourced hale gelecek.
// Token'ı bu servis basar (kimliğin sahibi kim, token'ı o verir);
// gateway yalnızca doğrular — ikisi HMAC sırrını paylaşır (RS256/JWKS
// ayrımı Faz 6'da).
package player

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/auth"
)

const maxNameLen = 24

type Service struct {
	pb.UnimplementedPlayerServiceServer

	secret   []byte
	tokenTTL time.Duration

	mu      sync.Mutex
	players map[string]*pb.Player
	nextID  int64
}

func New(secret []byte) *Service {
	return &Service{
		secret:   secret,
		tokenTTL: 24 * time.Hour,
		players:  map[string]*pb.Player{},
	}
}

func (s *Service) CreatePlayer(_ context.Context, req *pb.CreatePlayerRequest) (*pb.CreatePlayerResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" || len([]rune(name)) > maxNameLen {
		return nil, status.Errorf(codes.InvalidArgument, "name must be 1-%d characters", maxNameLen)
	}

	s.mu.Lock()
	s.nextID++
	id := fmt.Sprintf("p-%d", s.nextID)
	s.players[id] = &pb.Player{
		PlayerId:      id,
		Name:          name,
		CreatedAtUnix: time.Now().Unix(),
	}
	s.mu.Unlock()

	token, err := auth.Sign(s.secret, auth.Claims{
		Sub:  id,
		Name: name,
		Exp:  time.Now().Add(s.tokenTTL).Unix(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sign token: %v", err)
	}
	return &pb.CreatePlayerResponse{PlayerId: id, Token: token}, nil
}

func (s *Service) GetPlayer(_ context.Context, req *pb.GetPlayerRequest) (*pb.GetPlayerResponse, error) {
	s.mu.Lock()
	p, ok := s.players[req.GetPlayerId()]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "player %q not found", req.GetPlayerId())
	}
	return &pb.GetPlayerResponse{Player: p}, nil
}
