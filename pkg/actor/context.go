package actor

// Context, aktöre çalışma anında sağlanan API'dir: işlenen mesaj, gönderen,
// kendi Ref'i ve çocuk yaratma. Yalnızca Receive/PreStart/PostStop çağrısı
// sırasında ve aktörün kendi goroutine'inde geçerlidir; saklanmamalı,
// başka goroutine'e verilmemelidir.
type Context struct {
	p       *process
	message any
	sender  *Ref
}

// Self, aktörün kendi Ref'i.
func (c *Context) Self() *Ref { return c.p.ref }

// System, aktörün bağlı olduğu sistem.
func (c *Context) System() *System { return c.p.system }

// Message, şu an işlenen mesaj (lifecycle hook'larında nil).
func (c *Context) Message() any { return c.message }

// Sender, mesaj ctx.Send ile gönderildiyse gönderen aktörün Ref'i;
// sistem dışından Ref.Send ile geldiyse nil.
func (c *Context) Sender() *Ref { return c.sender }

// Send, mesajı bu aktörü Sender olarak işaretleyerek gönderir.
func (c *Context) Send(to *Ref, msg any) {
	if to == nil {
		return
	}
	to.p.sendUser(envelope{msg: msg, sender: c.p.ref})
}

// Spawn, bu aktörün çocuğu olarak yeni bir aktör başlatır. Çocuklar,
// ebeveyn durduğunda veya restart olduğunda otomatik durdurulur.
func (c *Context) Spawn(props Props) (*Ref, error) {
	return c.p.spawnChild(props)
}
