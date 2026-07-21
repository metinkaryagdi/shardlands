package actor

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Kontrol mesajları: framework'ün kendi iç protokolü. Kullanıcı
// mailbox'ından ayrı, unbounded bir kuyruktan ve öncelikli işlenir.
type ctrlStop struct{}
type ctrlChildStopped struct{ name string }
type ctrlChildEscalated struct {
	child  *Ref
	reason error
}

// process, bir aktörün çalışma zamanı iskeletidir: goroutine, mailbox,
// kontrol kuyruğu, çocuklar ve restart muhasebesi. Aktör instance'ı
// restart'ta değişir; process (ve dolayısıyla Ref) sabit kalır — dışarıya
// verdiğimiz Ref'ler restart'tan sonra da geçerlidir.
type process struct {
	system *System
	props  Props
	parent *process
	name   string
	ref    *Ref
	actor  Actor
	ctx    *Context

	user      *userMailbox
	ctrl      *ctrlQueue
	stoppedCh chan struct{}
	dead      atomic.Bool // sendUser'ın hızlı yolu; stoppedCh kapanmadan önce set edilir
	exited    bool        // yalnızca process goroutine'i okur/yazar

	childrenMu sync.Mutex // guardian'a dış goroutine'lerden Spawn gelebilir
	children   map[string]*Ref

	restarts []time.Time // Window içindeki restart zamanları
}

func newProcess(system *System, parent *process, props Props, name string) *process {
	size := props.MailboxSize
	if size <= 0 {
		size = 64
	}
	p := &process{
		system:    system,
		props:     props,
		parent:    parent,
		name:      name,
		user:      newUserMailbox(size),
		ctrl:      newCtrlQueue(),
		stoppedCh: make(chan struct{}),
		children:  map[string]*Ref{},
	}
	path := "/" + name
	if parent != nil {
		path = parent.ref.path + "/" + name
	}
	p.ref = &Ref{path: path, p: p}
	p.ctx = &Context{p: p}
	p.actor = props.Producer()
	return p
}

// run, aktörün ana döngüsüdür; kendi goroutine'inde çalışır.
func (p *process) run() {
	if err := p.safePreStart(); err != nil {
		p.handleFailure(err)
	}
	for !p.exited {
		// Kontrol kuyruğu her zaman önceliklidir: dolu bir mailbox'ın
		// arkasında Stop veya escalation bekletilmez.
		if c, ok := p.ctrl.pop(); ok {
			p.handleCtrl(c)
			continue
		}
		if env, ok := p.user.tryPop(); ok {
			p.invoke(env)
			continue
		}
		// İki kuyruk da boş: ilk sinyale kadar uyu. Sinyaller
		// coalesced olduğundan uyanınca başa dönüp tekrar denenir.
		select {
		case <-p.ctrl.wait():
		case <-p.user.wait():
		}
	}
}

func (p *process) handleCtrl(c any) {
	switch m := c.(type) {
	case ctrlStop:
		p.shutdown()
	case ctrlChildStopped:
		p.childrenMu.Lock()
		delete(p.children, m.name)
		p.childrenMu.Unlock()
	case ctrlChildEscalated:
		// Çocuğun hatası artık bizim hatamız: kendi stratejimiz karar verir.
		p.handleFailure(fmt.Errorf("child %s escalated: %w", m.child.Path(), m.reason))
	}
}

func (p *process) invoke(env envelope) {
	if _, ok := env.msg.(poisonPill); ok {
		p.shutdown()
		return
	}
	if err := p.safeReceive(env); err != nil {
		p.handleFailure(err)
	}
}

// safeReceive, kullanıcı kodundaki panic'i error'a çevirir; goroutine'i
// (dolayısıyla tüm süreci) çökertmek yerine supervision'a teslim eder.
func (p *process) safeReceive(env envelope) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("actor %s panicked: %v", p.ref.path, r)
		}
	}()
	p.ctx.message = env.msg
	p.ctx.sender = env.sender
	p.actor.Receive(p.ctx)
	return nil
}

func (p *process) handleFailure(reason error) {
	switch p.props.Supervision.decide(reason) {
	case DirectiveResume:
		// Hata yutulur; aynı instance sıradaki mesajla devam eder.
	case DirectiveStop:
		p.shutdown()
	case DirectiveEscalate:
		p.shutdown()
		if p.parent != nil {
			p.parent.ctrl.push(ctrlChildEscalated{child: p.ref, reason: reason})
		}
	default: // DirectiveRestart
		if !p.allowRestart() {
			p.shutdown()
			return
		}
		p.restartActor()
	}
}

// allowRestart, kayan pencere içindeki restart sayısını sınırlar; sınır
// aşılırsa false döner ve aktör durdurulur (restart fırtınası kesilir).
func (p *process) allowRestart() bool {
	max, win := p.props.Supervision.limits()
	now := time.Now()
	kept := p.restarts[:0]
	for _, t := range p.restarts {
		if now.Sub(t) < win {
			kept = append(kept, t)
		}
	}
	p.restarts = append(kept, now)
	return len(p.restarts) <= max
}

// restartActor: çocuklar durdurulur, eski instance'a PostStop ile veda
// edilir, Producer'dan taze instance alınır. Mailbox'a DOKUNULMAZ —
// kuyruktaki mesajlar yeni instance tarafından işlenmeye devam eder.
func (p *process) restartActor() {
	p.stopChildrenAndWait()
	p.safePostStop()
	p.actor = p.props.Producer()
	if err := p.safePreStart(); err != nil {
		p.handleFailure(err) // özyineleme allowRestart ile sınırlı
	}
}

// shutdown, aktörü kalıcı olarak durdurur. Sıra önemli: önce çocuklar
// (aşağıdan yukarıya temizlik), sonra PostStop, sonra dead işareti +
// stoppedCh kapanışı, en son mailbox artıklarının dead letter'a boşaltılması.
func (p *process) shutdown() {
	if p.exited {
		return
	}
	p.exited = true
	p.stopChildrenAndWait()
	p.safePostStop()
	// Sıra önemli: dead bayrağı → drain → close(stoppedCh) → son drain.
	// Drain close'tan ÖNCE biter ki Stopped() ateşlendiğinde dead letter
	// muhasebesi tamamlanmış olsun (testler ve gözlemciler buna güvenir).
	// Son drain, ilk drain sırasında spaceFreed sinyaliyle uyanıp mesaj
	// itmeyi başarmış Block üreticilerinin artıklarını yakalar.
	p.dead.Store(true)
	p.drainToDeadLetters()
	close(p.stoppedCh)
	p.drainToDeadLetters()
	if p.parent != nil {
		p.parent.ctrl.push(ctrlChildStopped{name: p.name})
	}
}

func (p *process) drainToDeadLetters() {
	for {
		env, ok := p.user.tryPop()
		if !ok {
			return
		}
		p.system.deadLetter(p.ref, env.msg)
	}
}

func (p *process) stopChildrenAndWait() {
	p.childrenMu.Lock()
	refs := make([]*Ref, 0, len(p.children))
	for _, r := range p.children {
		refs = append(refs, r)
	}
	p.children = map[string]*Ref{}
	p.childrenMu.Unlock()
	for _, r := range refs {
		r.Stop()
	}
	for _, r := range refs {
		<-r.Stopped()
	}
}

// sendUser, mesajı mailbox'a bırakır. Aktör ölüyse veya (Block modunda)
// beklerken ölürse mesaj dead letter olur; stoppedCh sayesinde ölü bir
// aktörün dolu mailbox'ına gönderen sonsuza dek bloklanmaz.
func (p *process) sendUser(env envelope) {
	if p.dead.Load() {
		p.system.deadLetter(p.ref, env.msg)
		return
	}
	if p.user.tryPush(env) {
		return
	}
	if p.props.Overflow == DropNewest {
		p.system.deadLetter(p.ref, env.msg)
		return
	}
	// Block: yer açılana ya da aktör ölene kadar bekle-ve-tekrar-dene.
	// space sinyali coalesced olduğundan uyanmak garanti, slot kapmak
	// değil (başka üretici önce davranabilir); o yüzden döngü şart.
	for {
		select {
		case <-p.user.spaceFreed():
			// Uyandıran, ölmekte olan aktörün drain'i olabilir; ölüye
			// itmeye çalışmak yerine dead letter'a düş.
			if p.dead.Load() {
				p.system.deadLetter(p.ref, env.msg)
				return
			}
		case <-p.stoppedCh:
			p.system.deadLetter(p.ref, env.msg)
			return
		}
		if p.user.tryPush(env) {
			return
		}
	}
}

func (p *process) spawnChild(props Props) (*Ref, error) {
	if props.Producer == nil {
		return nil, errors.New("actor: Props.Producer is required")
	}
	if p.dead.Load() {
		return nil, fmt.Errorf("actor: %s is stopped, cannot spawn child", p.ref.path)
	}
	name := props.Name
	if name == "" {
		name = fmt.Sprintf("actor-%d", p.system.nextID.Add(1))
	}
	if strings.ContainsRune(name, '/') {
		return nil, fmt.Errorf("actor: name %q must not contain '/'", name)
	}
	p.childrenMu.Lock()
	if _, ok := p.children[name]; ok {
		p.childrenMu.Unlock()
		return nil, fmt.Errorf("actor: name %q already in use under %s", name, p.ref.path)
	}
	child := newProcess(p.system, p, props, name)
	p.children[name] = child.ref
	p.childrenMu.Unlock()
	go child.run()
	return child.ref, nil
}

func (p *process) safePreStart() (err error) {
	h, ok := p.actor.(PreStarter)
	if !ok {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("actor %s panicked in PreStart: %v", p.ref.path, r)
		}
	}()
	p.ctx.message, p.ctx.sender = nil, nil
	h.PreStart(p.ctx)
	return nil
}

// safePostStop: temizlik kancasındaki panic durdurmayı engelleyemez,
// yutulur.
func (p *process) safePostStop() {
	h, ok := p.actor.(PostStopper)
	if !ok {
		return
	}
	defer func() { _ = recover() }()
	p.ctx.message, p.ctx.sender = nil, nil
	h.PostStop(p.ctx)
}
