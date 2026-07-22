// Package transport, oyun kare akışını WebSocket (TCP) ve QUIC
// (güvenilmez datagram) üzerinden karşılaştıran deney düzeneğidir.
//
// Ölçmek istediğimiz: HEAD-OF-LINE (HOL) BLOCKING. TCP tek sıralı bayt
// akışıdır; kaybolan bir segment yeniden iletilene kadar ARKASINDAN
// GELEN ve zaten ulaşmış veri de uygulamaya teslim EDİLEMEZ. Oyun için
// bu tam ters bir davranıştır: eski bir kare yüzünden yeni kare bekler,
// oysa yeni kare eskisini zaten geçersiz kılar.
//
// QUIC'in cevabı: bağımsız stream'ler ve GÜVENİLMEZ DATAGRAM'lar. Kayıp
// bir datagram kaybolur, kuyruk tıkanmaz — sıradaki kare zamanında gelir.
//
// Kayıp simülasyonu dürüstlük notu: iki taşıma farklı biçimde bozulur.
//   - UDP/QUIC yolunda proxy paketleri GERÇEKTEN düşürür.
//   - TCP yolunda bayt düşürmek akışı bozardı (TCP uçtan uca güvenilir);
//     bu yüzden proxy bir parçayı GECİKTİRİR. Bu, yeniden iletim
//     beklemesinin gözlemlenebilir sonucunu birebir üretir: sıralı
//     teslim zorunluluğu yüzünden arkadaki her şey bekler.
package transport

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// StallProxy, TCP bağlantılarını ileten ve her N. parçayı belirli bir
// süre GECİKTİREN proxy'dir (yeniden iletim beklemesinin emülasyonu).
type StallProxy struct {
	ln      net.Listener
	target  string
	everyN  int64
	skip    int64 // ısınma: ilk N parçaya dokunma (el sıkışma bozulmasın)
	stall   time.Duration
	counter atomic.Int64
	stalls  atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup
	closeMu sync.Mutex
	conns   []net.Conn
}

// NewStallProxy, target'a ileten bir TCP proxy başlatır. skipFirst,
// bozulmadan geçirilecek ilk parça sayısıdır (WS el sıkışması).
func NewStallProxy(target string, everyN int, stall time.Duration, skipFirst int) (*StallProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &StallProxy{ln: ln, target: target, everyN: int64(everyN), stall: stall, skip: int64(skipFirst)}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

func (p *StallProxy) Addr() string { return p.ln.Addr().String() }

// Stalls, uygulanan gecikme sayısı.
func (p *StallProxy) Stalls() int64 { return p.stalls.Load() }

func (p *StallProxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		p.track(c, up)
		// İstemci→sunucu yönü düz aktarılır.
		go func() { io.Copy(up, c); up.Close() }()
		// Sunucu→istemci yönü: kare akışı burada, gecikme burada uygulanır.
		go func() { p.copyWithStalls(c, up); c.Close() }()
	}
}

func (p *StallProxy) track(cs ...net.Conn) {
	p.closeMu.Lock()
	p.conns = append(p.conns, cs...)
	p.closeMu.Unlock()
}

// copyWithStalls, her everyN. okumayı stall süresi kadar geciktirir.
// Aktarım SIRALI olduğu için geciken parçanın arkasındaki her şey de
// bekler — HOL blocking'in ta kendisi.
func (p *StallProxy) copyWithStalls(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if c := p.counter.Add(1); p.everyN > 0 && c > p.skip && c%p.everyN == 0 {
				p.stalls.Add(1)
				time.Sleep(p.stall)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *StallProxy) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.ln.Close()
	p.closeMu.Lock()
	for _, c := range p.conns {
		c.Close()
	}
	p.closeMu.Unlock()
	p.wg.Wait()
}

// DropProxy, UDP paketlerini ileten ve her N. paketi GERÇEKTEN düşüren
// proxy'dir (QUIC yolu için gerçek kayıp).
type DropProxy struct {
	conn    *net.UDPConn
	target  *net.UDPAddr
	everyN  int64
	skip    int64 // ısınma: ilk N paketi düşürme (QUIC el sıkışması)
	counter atomic.Int64
	drops   atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup
}

// NewDropProxy, target'a ileten bir UDP proxy başlatır. skipFirst,
// dokunulmadan geçirilecek ilk paket sayısıdır (QUIC el sıkışması).
// Yalnız tek istemci içindir (deney düzeneği).
func NewDropProxy(target string, everyN, skipFirst int) (*DropProxy, error) {
	taddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return nil, err
	}
	p := &DropProxy{conn: c, target: taddr, everyN: int64(everyN), skip: int64(skipFirst)}
	p.wg.Add(1)
	go p.run()
	return p, nil
}

func (p *DropProxy) Addr() string { return p.conn.LocalAddr().String() }

// Drops, düşürülen paket sayısı.
func (p *DropProxy) Drops() int64 { return p.drops.Load() }

func (p *DropProxy) run() {
	defer p.wg.Done()
	buf := make([]byte, 2048)

	var (
		mu         sync.Mutex
		clientAddr *net.UDPAddr
		upstream   *net.UDPConn
	)
	// Sunucudan gelenleri istemciye ileten yön.
	startUpstreamReader := func(up *net.UDPConn) {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			b := make([]byte, 2048)
			for {
				n, err := up.Read(b)
				if err != nil {
					return
				}
				// Sunucu→istemci yönünde kayıp uygula (kare akışı bu yön).
				if c := p.counter.Add(1); p.everyN > 0 && c > p.skip && c%p.everyN == 0 {
					p.drops.Add(1)
					continue
				}
				mu.Lock()
				ca := clientAddr
				mu.Unlock()
				if ca != nil {
					p.conn.WriteToUDP(b[:n], ca)
				}
			}
		}()
	}

	for {
		n, addr, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			if upstream != nil {
				upstream.Close()
			}
			return
		}
		mu.Lock()
		if clientAddr == nil {
			clientAddr = addr
		}
		mu.Unlock()
		if upstream == nil {
			up, derr := net.DialUDP("udp", nil, p.target)
			if derr != nil {
				continue
			}
			upstream = up
			startUpstreamReader(up)
		}
		upstream.Write(buf[:n])
	}
}

func (p *DropProxy) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.conn.Close()
	p.wg.Wait()
}
