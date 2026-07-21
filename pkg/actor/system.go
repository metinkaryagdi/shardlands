package actor

import "sync/atomic"

// System, aktör ağacının köküdür. Doğrudan System.Spawn ile yaratılan
// aktörler, görünmez bir "user guardian" aktörünün çocuğudur; böylece
// üst seviye aktörler de aynı stop/escalation mekanizmasından geçer
// (Erlang/Akka'daki root guardian fikri).
type System struct {
	name        string
	guardian    *process
	nextID      atomic.Int64
	deadLetters atomic.Int64
	dlHandler   func(to *Ref, msg any)
}

// Option, NewSystem için yapılandırma.
type Option func(*System)

// WithDeadLetterHandler, her dead letter için çağrılacak kancayı ayarlar.
// Farklı goroutine'lerden eşzamanlı çağrılabilir; thread-safe olmalıdır.
func WithDeadLetterHandler(f func(to *Ref, msg any)) Option {
	return func(s *System) { s.dlHandler = f }
}

// guardianActor mesajları yok sayar; ona escalate eden üst seviye bir
// aktör zaten durmuştur, guardian Resume ile yoluna devam eder.
type guardianActor struct{}

func (guardianActor) Receive(*Context) {}

func NewSystem(name string, opts ...Option) *System {
	s := &System{name: name}
	for _, o := range opts {
		o(s)
	}
	s.guardian = newProcess(s, nil, Props{
		Producer: func() Actor { return guardianActor{} },
		Supervision: Strategy{
			Decider: func(error) Directive { return DirectiveResume },
		},
	}, "user")
	go s.guardian.run()
	return s
}

// Spawn, üst seviye bir aktör başlatır (guardian'ın çocuğu olarak).
func (s *System) Spawn(props Props) (*Ref, error) {
	return s.guardian.spawnChild(props)
}

// Shutdown, tüm aktör ağacını (yapraklardan köke doğru) durdurur ve bekler.
func (s *System) Shutdown() {
	s.guardian.ref.Stop()
	<-s.guardian.ref.Stopped()
}

// DeadLetterCount: teslim edilememiş (ölü aktöre gönderilen, taşan veya
// stop sırasında mailbox'ta kalan) mesaj sayısı.
func (s *System) DeadLetterCount() int64 { return s.deadLetters.Load() }

func (s *System) deadLetter(to *Ref, msg any) {
	s.deadLetters.Add(1)
	if s.dlHandler != nil {
		s.dlHandler(to, msg)
	}
}
