package actor

// Ref, bir aktöre konum-bağımsız referanstır. Aktörün kendisine (ve
// state'ine) asla doğrudan erişilmez; tüm etkileşim Ref üzerinden mesajla
// olur. İleride bu dolaylama, Ref'in ağ üzerindeki bir aktörü
// göstermesine de izin verecek.
type Ref struct {
	path string
	p    *process
}

// Path, aktörün ağaçtaki adresidir (örn. "/user/world/session-42").
func (r *Ref) Path() string { return r.path }

// Send mesajı aktörün mailbox'ına bırakır (gönderen bilgisi olmadan).
// Aktör içinden gönderirken ctx.Send kullan ki Sender taşınsın.
func (r *Ref) Send(msg any) { r.p.sendUser(envelope{msg: msg}) }

// Stop aktörü en kısa sürede durdurur: kontrol kuyruğundan gider, işlenmekte
// olan mesaj biter bitmez mailbox'ta bekleyenler işlenmeden durdurulur
// (bekleyenler dead letter olur).
func (r *Ref) Stop() { r.p.ctrl.push(ctrlStop{}) }

// Poison nazik durdurmadır: mailbox'ta kendinden önce sıraya girmiş tüm
// mesajlar işlendikten sonra aktör durur.
func (r *Ref) Poison() { r.p.sendUser(envelope{msg: poisonPill{}}) }

// Stopped, aktör tamamen durduğunda (çocuklar durdu + PostStop çalıştı)
// kapanan bir kanal döner. close(ch) işlemi aktörün son yazmalarından
// sonra olduğundan, bu kanaldan okuduktan sonra aktörün bıraktığı veriye
// bakmak data race değildir (happens-before).
func (r *Ref) Stopped() <-chan struct{} { return r.p.stoppedCh }
