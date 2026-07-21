package actor

// Actor bir aktörün davranışıdır: her çağrıda tek bir mesaj işler.
// Receive içindeki panic'ler framework tarafından yakalanır ve
// supervision stratejisine iletilir.
type Actor interface {
	Receive(ctx *Context)
}

// ReceiverFunc, tek fonksiyonluk aktörler için adaptördür.
type ReceiverFunc func(ctx *Context)

func (f ReceiverFunc) Receive(ctx *Context) { f(ctx) }

// PreStarter uygulanırsa, aktör instance'ı mesaj işlemeye başlamadan önce
// (ilk başlatmada ve her restart'ta) çağrılır.
type PreStarter interface{ PreStart(ctx *Context) }

// PostStopper uygulanırsa, aktör instance'ı durduğunda ve restart'ta eski
// instance atılmadan önce çağrılır. Kaynak temizliği burada yapılır.
type PostStopper interface{ PostStop(ctx *Context) }
