package arena

import (
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
)

// Server, bir arena instance'ını gRPC üzerinden sunar (arena Pod'u).
//
// Gateway, oyuncunun WS bağlantısını kendi tutar ve komutları buraya
// VEKİL EDER; kareler ters yönde akar. İstemci sözleşmesi değişmez,
// iç hop (gateway→arena) service mesh'in koruyabileceği normal bir
// gRPC çağrısı olur.
type Server struct {
	pb.UnimplementedArenaServiceServer
	a *Arena
}

func NewServer(a *Arena) *Server { return &Server{a: a} }

// Arena, sunulan instance (gözlem/test).
func (s *Server) Arena() *Arena { return s.a }

// streamSink, snapshot'ları gRPC akışına taşır. Deliver BLOKLAMAZ:
// tampon doluysa kare DÜŞER (arena profili: eskimiş kareyi bekleme).
type streamSink struct {
	ch      chan Snapshot
	once    sync.Once
	dropped int
	mu      sync.Mutex
}

func newStreamSink() *streamSink {
	return &streamSink{ch: make(chan Snapshot, 8)}
}

func (s *streamSink) Deliver(snap Snapshot) {
	select {
	case s.ch <- snap:
	default:
		s.mu.Lock()
		s.dropped++
		s.mu.Unlock()
	}
}

func (s *streamSink) close() { s.once.Do(func() { close(s.ch) }) }

// Play, oyuncu başına çift yönlü akıştır. İlk mesaj Join olmalıdır.
func (s *Server) Play(stream pb.ArenaService_PlayServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	join := first.GetJoin()
	if join == nil || join.GetPlayerId() == "" {
		return status.Error(codes.InvalidArgument, "first message must be Join with player_id")
	}
	playerID := join.GetPlayerId()
	if !s.a.HasPlayer(playerID) {
		return status.Errorf(codes.PermissionDenied, "player %s is not in arena %s", playerID, s.a.ID())
	}

	sink := newStreamSink()
	s.a.SetSink(playerID, sink)
	defer func() {
		s.a.SetSink(playerID, nil)
		sink.close()
	}()

	// Yazıcı: snapshot'ları akışa bas.
	sendErr := make(chan error, 1)
	go func() {
		for snap := range sink.ch {
			if err := stream.Send(&pb.PlayResponse{Snapshot: toProtoSnapshot(snap)}); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	// Okuyucu: komutları arenanın kilitsiz kuyruğuna it.
	for {
		select {
		case err := <-sendErr:
			return err
		default:
		}
		req, err := stream.Recv()
		if err == io.EOF {
			// İstemci kapattı: oyuncu ayrıldı say.
			s.a.Push(Command{PlayerID: playerID, Kind: CmdLeave})
			return nil
		}
		if err != nil {
			s.a.Push(Command{PlayerID: playerID, Kind: CmdLeave})
			return err
		}
		if c := req.GetCommand(); c != nil {
			s.a.Push(fromProtoCommand(playerID, c))
		}
	}
}

// ---- tel ↔ iç tip dönüşümleri ----

func toProtoSnapshot(s Snapshot) *pb.ArenaSnapshot {
	out := &pb.ArenaSnapshot{
		ArenaId: s.ArenaID, Tick: s.Tick, RemainingMs: s.RemainingMs,
		Over: s.Over, WinnerTeam: int32(s.WinnerTeam),
		Players:     make([]*pb.ArenaPlayerState, len(s.Players)),
		Projectiles: make([]*pb.ArenaProjectile, len(s.Projectiles)),
	}
	for i, p := range s.Players {
		out.Players[i] = &pb.ArenaPlayerState{
			Id: p.ID, Name: p.Name, Team: int32(p.Team),
			X: p.X, Y: p.Y, Health: int32(p.Health), Alive: p.Alive,
		}
	}
	for i, pr := range s.Projectiles {
		out.Projectiles[i] = &pb.ArenaProjectile{
			Id: pr.ID, Team: int32(pr.Team), X: pr.X, Y: pr.Y,
		}
	}
	return out
}

// FromProtoSnapshot, tel biçimini iç tipe çevirir (gateway tarafı).
func FromProtoSnapshot(p *pb.ArenaSnapshot) Snapshot {
	if p == nil {
		return Snapshot{}
	}
	s := Snapshot{
		ArenaID: p.GetArenaId(), Tick: p.GetTick(), RemainingMs: p.GetRemainingMs(),
		Over: p.GetOver(), WinnerTeam: int(p.GetWinnerTeam()),
		Players:     make([]PlayerState, len(p.GetPlayers())),
		Projectiles: make([]ProjectileState, len(p.GetProjectiles())),
	}
	for i, pp := range p.GetPlayers() {
		s.Players[i] = PlayerState{
			ID: pp.GetId(), Name: pp.GetName(), Team: int(pp.GetTeam()),
			X: pp.GetX(), Y: pp.GetY(), Health: int(pp.GetHealth()), Alive: pp.GetAlive(),
		}
	}
	for i, pr := range p.GetProjectiles() {
		s.Projectiles[i] = ProjectileState{
			ID: pr.GetId(), Team: int(pr.GetTeam()), X: pr.GetX(), Y: pr.GetY(),
		}
	}
	return s
}

func fromProtoCommand(playerID string, c *pb.ArenaCommand) Command {
	return Command{
		PlayerID: playerID, Kind: CmdKind(c.GetKind()),
		Up: c.GetUp(), Down: c.GetDown(), Left: c.GetLeft(), Right: c.GetRight(),
		AimX: c.GetAimX(), AimY: c.GetAimY(),
	}
}

// ToProtoCommand, iç komutu tel biçimine çevirir (gateway tarafı).
func ToProtoCommand(c Command) *pb.ArenaCommand {
	return &pb.ArenaCommand{
		Kind: int32(c.Kind), Up: c.Up, Down: c.Down, Left: c.Left, Right: c.Right,
		AimX: c.AimX, AimY: c.AimY,
	}
}

// ErrNoPlayers: Pod ortamında oyuncu tanımı eksik.
var ErrNoPlayers = errors.New("arena: no players configured")
