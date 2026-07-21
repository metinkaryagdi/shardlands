package matchmaking

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
)

func TestEnqueuePositionsAndIdempotency(t *testing.T) {
	s := New()
	ctx := context.Background()

	r1, err := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "1v1"})
	if err != nil || r1.Position != 1 {
		t.Fatalf("first = %v pos=%d, want pos 1", err, r1.GetPosition())
	}
	r2, _ := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-2", Mode: "1v1"})
	if r2.Position != 2 {
		t.Fatalf("second pos = %d, want 2", r2.Position)
	}
	// Aynı oyuncu tekrar sorarsa (retry) sırası değişmemeli.
	again, _ := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "1v1"})
	if again.Position != 1 {
		t.Fatalf("retry pos = %d, want 1 (idempotent)", again.Position)
	}
	// Farklı mod ayrı kuyruk.
	other, _ := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "2v2"})
	if other.Position != 1 {
		t.Fatalf("2v2 pos = %d, want 1 (separate queue)", other.Position)
	}
}

func TestEnqueueValidation(t *testing.T) {
	s := New()
	ctx := context.Background()
	_, err := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "", Mode: "1v1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty player: %v, want InvalidArgument", status.Code(err))
	}
	_, err = s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "5v5"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad mode: %v, want InvalidArgument", status.Code(err))
	}
}
