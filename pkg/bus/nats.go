package bus

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Konu düzeni: shardlands.events.<tip>, DLQ: shardlands.dlq.<durable>
const (
	SubjectPrefix = "shardlands.events."
	dlqPrefix     = "shardlands.dlq."
	streamName    = "SHARDLANDS"
	dlqStreamName = "SHARDLANDS_DLQ"
)

// EventSubject, event tipi için konu adı üretir.
func EventSubject(eventType string) string {
	return SubjectPrefix + sanitize(eventType)
}

// sanitize, konu adında geçersiz karakterleri temizler.
func sanitize(s string) string {
	r := strings.NewReplacer(".", "_", " ", "_", "*", "_", ">", "_")
	return r.Replace(s)
}

// Embedded, gömülü NATS sunucusudur (tek süreçli kurulum ve testler
// için; Faz 6'da yerini gerçek bir kümeye bırakacak).
type Embedded struct{ s *server.Server }

// StartEmbedded, JetStream açık gömülü bir NATS sunucusu başlatır.
func StartEmbedded(storeDir string) (*Embedded, error) {
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // boş port seç
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		return nil, fmt.Errorf("bus: embedded NATS did not start")
	}
	return &Embedded{s: s}, nil
}

func (e *Embedded) URL() string { return e.s.ClientURL() }

func (e *Embedded) Shutdown() {
	e.s.Shutdown()
	e.s.WaitForShutdown()
}

// natsBus, Bus'ın JetStream implementasyonu.
type natsBus struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	str jetstream.Stream
	dlq jetstream.Stream
}

// Connect, verilen URL'e bağlanır ve stream'leri kurar.
func Connect(url string) (Bus, error) {
	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(200*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	str, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{SubjectPrefix + ">"},
		Storage:  jetstream.FileStorage,
		// Yayın tarafı dedupe penceresi: aynı Msg-Id kısa sürede iki kez
		// yayınlanırsa ikincisi yutulur (outbox relay restart'ında işe
		// yarar; garanti DEĞİL — tüketici yine idempotent olmalı).
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	dlq, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     dlqStreamName,
		Subjects: []string{dlqPrefix + ">"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &natsBus{nc: nc, js: js, str: str, dlq: dlq}, nil
}

func (b *natsBus) Publish(ctx context.Context, subject, id string, data []byte) error {
	var opts []jetstream.PublishOpt
	if id != "" {
		opts = append(opts, jetstream.WithMsgID(id))
	}
	_, err := b.js.Publish(ctx, subject, data, opts...)
	return err
}

type natsSub struct {
	cc      jetstream.ConsumeContext
	stopped atomic.Bool
}

func (s *natsSub) Stop() {
	if s.stopped.CompareAndSwap(false, true) {
		s.cc.Stop()
	}
}

func (b *natsBus) Subscribe(opts SubscribeOptions, h Handler) (Subscription, error) {
	opts.withDefaults()
	if opts.Durable == "" {
		return nil, fmt.Errorf("bus: Durable is required")
	}
	filter := opts.Filter
	if filter == "" {
		filter = SubjectPrefix + ">"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, err := b.str.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       opts.Durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       opts.AckWait,
		// MaxDeliver'ı bus'ın kendisinde SINIRSIZ bırakıp DLQ kararını
		// kendimiz veriyoruz: böylece "vazgeçtim" anında mesajı DLQ'ya
		// TAŞIYABİLİYORUZ (JetStream'de max-deliver aşımı mesajı
		// sessizce düşürür, bir yere koymaz).
		MaxAckPending: opts.MaxInFlight,
	})
	if err != nil {
		return nil, err
	}

	dlqSubject := dlqPrefix + sanitize(opts.Durable)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		md, mdErr := msg.Metadata()
		deliveries := 1
		if mdErr == nil {
			deliveries = int(md.NumDelivered)
		}
		m := Message{
			Subject:    msg.Subject(),
			Data:       msg.Data(),
			ID:         msg.Headers().Get(jetstream.MsgIDHeader),
			Deliveries: deliveries,
		}

		hctx, hcancel := context.WithTimeout(context.Background(), opts.AckWait)
		err := h(hctx, m)
		hcancel()
		if err == nil {
			msg.Ack()
			return
		}
		if deliveries >= opts.MaxDeliver {
			// Zehirli mesaj: DLQ'ya taşı ve ack'le ki akış tıkanmasın.
			pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
			b.Publish(pctx, dlqSubject, "", msg.Data())
			pcancel()
			msg.Ack()
			return
		}
		msg.NakWithDelay(opts.Backoff(deliveries))
	})
	if err != nil {
		return nil, err
	}
	return &natsSub{cc: cc}, nil
}

func (b *natsBus) DeadLetters(durable string, h Handler) (Subscription, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := b.dlq.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "dlqreader_" + sanitize(durable),
		FilterSubject: dlqPrefix + sanitize(durable),
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, err
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		m := Message{Subject: msg.Subject(), Data: msg.Data(), Deliveries: 1}
		if err := h(context.Background(), m); err == nil {
			msg.Ack()
		} else {
			msg.Nak()
		}
	})
	if err != nil {
		return nil, err
	}
	return &natsSub{cc: cc}, nil
}

func (b *natsBus) Close() error {
	b.nc.Close()
	return nil
}
