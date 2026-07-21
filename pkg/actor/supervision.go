package actor

import "time"

// Directive, bir aktör hata verdiğinde (panic) ne yapılacağını söyler.
type Directive int

const (
	// DirectiveRestart: aktör instance'ı yenisiyle değiştirilir (state
	// sıfırlanır), mailbox korunur. Varsayılan davranış.
	DirectiveRestart Directive = iota
	// DirectiveResume: hata yutulur, aynı instance sıradaki mesajla devam
	// eder (state korunur). Yalnızca hatanın state'i bozmadığından
	// eminken kullanılmalı.
	DirectiveResume
	// DirectiveStop: aktör kalıcı olarak durdurulur.
	DirectiveStop
	// DirectiveEscalate: aktör durur ve hata ebeveynin hatasıymış gibi
	// ebeveynin stratejisine iletilir.
	DirectiveEscalate
)

// Decider, hata sebebine bakarak bir Directive seçer.
type Decider func(reason error) Directive

// Strategy, bir aktörün KENDİ hataları için restart politikasıdır
// (Erlang child spec'indeki restart tanımına benzer; Akka'da bu karar
// ebeveyndedir — burada spawn eden, Props ile deklare eder).
type Strategy struct {
	// Decider nil ise her hata için DirectiveRestart uygulanır.
	Decider Decider
	// Window içinde MaxRestarts'tan fazla restart gerekirse aktör
	// durdurulur (restart fırtınasını keser). Sıfır değerler için
	// varsayılan: 10 restart / 10 saniye.
	MaxRestarts int
	Window      time.Duration
}

func (s Strategy) decide(reason error) Directive {
	if s.Decider == nil {
		return DirectiveRestart
	}
	return s.Decider(reason)
}

func (s Strategy) limits() (int, time.Duration) {
	max, win := s.MaxRestarts, s.Window
	if max <= 0 {
		max = 10
	}
	if win <= 0 {
		win = 10 * time.Second
	}
	return max, win
}
