package player

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/auth"
)

var secret = []byte("test-secret")

func TestCreatePlayerIssuesValidToken(t *testing.T) {
	s := New(secret)
	resp, err := s.CreatePlayer(context.Background(), &pb.CreatePlayerRequest{Name: "  metin  "})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := auth.Verify(secret, resp.Token)
	if err != nil {
		t.Fatalf("issued token must verify: %v", err)
	}
	if claims.Sub != resp.PlayerId || claims.Name != "metin" {
		t.Fatalf("claims = %+v, want sub=%s name=metin (trimmed)", claims, resp.PlayerId)
	}

	got, err := s.GetPlayer(context.Background(), &pb.GetPlayerRequest{PlayerId: resp.PlayerId})
	if err != nil {
		t.Fatal(err)
	}
	if got.Player.Name != "metin" {
		t.Fatalf("stored name = %q, want metin", got.Player.Name)
	}
}

func TestCreatePlayerValidation(t *testing.T) {
	s := New(secret)
	for _, name := range []string{"", "   ", strings.Repeat("x", 25)} {
		_, err := s.CreatePlayer(context.Background(), &pb.CreatePlayerRequest{Name: name})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("name %q: code = %v, want InvalidArgument", name, status.Code(err))
		}
	}
}

func TestGetPlayerNotFound(t *testing.T) {
	s := New(secret)
	_, err := s.GetPlayer(context.Background(), &pb.GetPlayerRequest{PlayerId: "p-999"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}

func TestPlayerIDsUnique(t *testing.T) {
	s := New(secret)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		resp, err := s.CreatePlayer(context.Background(), &pb.CreatePlayerRequest{Name: "x"})
		if err != nil {
			t.Fatal(err)
		}
		if seen[resp.PlayerId] {
			t.Fatalf("duplicate id %s", resp.PlayerId)
		}
		seen[resp.PlayerId] = true
	}
}
