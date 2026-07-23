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

	// keys, imzalama anahtar zinciri. Tek anahtar yerine zincir
	// tutmanın sebebi rotasyon: yeni anahtar devreye girdiğinde eski
	// token'lar geçerli kalmalı (pkg/auth/keyring.go).
	keys     *auth.Keyring
	tokenTTL time.Duration
	// instance, bu kopyayı diğerlerinden ayıran ön ek. Boşsa tek kopya
	// varsayılır ve kimlikler eskisi gibi "p-1" biçiminde üretilir.
	instance string

	mu      sync.Mutex
	players map[string]*pb.Player
	nextID  int64
}

// New, tek kopyalık servis kurar (tek süreç geliştirme ve testler).
func New(secret []byte) *Service { return NewInstance(secret, "") }

// NewKeyring, anahtar zinciriyle servis kurar — Vault'tan beslenen
// kurulumda kullanılan yapıcı budur. Zincir dışarıdan güncellenebilir
// olduğu için servis yeniden başlatılmadan anahtar dönebilir.
func NewKeyring(keys *auth.Keyring, instance string) *Service {
	return &Service{
		keys:     keys,
		tokenTTL: 24 * time.Hour,
		instance: instance,
		players:  map[string]*pb.Player{},
	}
}

// NewInstance, YATAY ÖLÇEKLENEBİLİR servis kurar.
//
// Neden ayrı bir yapıcı gerekti? Bu servis "durumsuz" görünüyordu ama
// gizli bir durumu vardı: artan bir sayaç. İki kopya aynı anda koşsa
// ikisi de "p-1" basar ve iki farklı oyuncu aynı kimliği taşır — token
// imzası geçerli olduğu için hata da vermez, sessizce yanlış olur.
//
// Ders: bir servisin ölçeklenip ölçeklenemeyeceğini "veritabanı var mı"
// diye bakarak anlayamazsın. Sayaçlar, rastgele tohumlar, yerel
// önbellekler ve zaman damgaları da durumdur.
//
// instance genelde Pod adından gelir (aşağı yönlü API); Pod adları bir
// namespace içinde benzersiz olduğu için ön ek de benzersizdir.
func NewInstance(secret []byte, instance string) *Service {
	return NewKeyring(auth.NewKeyring(secret), instance)
}

func (s *Service) CreatePlayer(_ context.Context, req *pb.CreatePlayerRequest) (*pb.CreatePlayerResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" || len([]rune(name)) > maxNameLen {
		return nil, status.Errorf(codes.InvalidArgument, "name must be 1-%d characters", maxNameLen)
	}

	s.mu.Lock()
	s.nextID++
	id := fmt.Sprintf("p-%d", s.nextID)
	if s.instance != "" {
		id = fmt.Sprintf("p-%s-%d", s.instance, s.nextID)
	}
	s.players[id] = &pb.Player{
		PlayerId:      id,
		Name:          name,
		CreatedAtUnix: time.Now().Unix(),
	}
	s.mu.Unlock()

	token, err := s.keys.Sign(auth.Claims{
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
