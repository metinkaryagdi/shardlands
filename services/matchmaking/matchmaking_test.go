package matchmaking

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
)

func testService() *Service {
	return New(NewMatcher(nil, NewLocalProvisioner(), &recordingAssigner{}))
}

// gRPC ucu: sıra numarası döner ve aynı oyuncu için idempotenttir.
func TestEnqueuePositionsAndIdempotency(t *testing.T) {
	s := testService()
	ctx := context.Background()

	r1, err := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "1v1"})
	if err != nil || r1.Position != 1 {
		t.Fatalf("first = %v pos=%d, want pos 1", err, r1.GetPosition())
	}
	again, _ := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "1v1"})
	if again.Position != 1 {
		t.Fatalf("retry pos = %d, want 1 (idempotent)", again.Position)
	}
	// Farklı mod ayrı kuyruk.
	other, err := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-2", Mode: "2v2"})
	if err != nil || other.Position != 1 {
		t.Fatalf("2v2 = %v pos=%d, want pos 1", err, other.GetPosition())
	}
}

func TestEnqueueValidation(t *testing.T) {
	s := testService()
	ctx := context.Background()
	if _, err := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "", Mode: "1v1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty player: %v, want InvalidArgument", status.Code(err))
	}
	if _, err := s.Enqueue(ctx, &pb.EnqueueRequest{PlayerId: "p-1", Mode: "5v5"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad mode: %v, want InvalidArgument", status.Code(err))
	}
}
