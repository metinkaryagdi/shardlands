package storage

import (
	"bufio"
	"errors"
	"io"
	"os"
)

// WAL (write-ahead log): memtable RAM'de yaşadığı için crash'te
// kaybolur; her Put/Delete önce bu append-only log'a yazılır. Açılışta
// log baştan oynatılarak (replay) memtable yeniden kurulur. Memtable
// SSTable'a flush edilince log'un işi biter ve sıfırlanır.
//
// Dayanıklılık kademeleri:
//   - sync=false (varsayılan): her kayıtta bufio flush → process crash
//     kayıpsız, OS/elektrik kesintisi son birkaç kaydı kaybedebilir.
//   - sync=true: her kayıtta fsync → elektrik kesintisine de dayanıklı,
//     ama her yazma bir disk turu bekler. Klasik dayanıklılık/gecikme
//     trade-off'u; Faz 2'de event store bunu grup commit'e evriltebilir.
type wal struct {
	f       *os.File
	bw      *bufio.Writer
	path    string
	sync    bool
	buf     []byte
	scratch []byte
}

func openWAL(path string, sync bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, bw: bufio.NewWriter(f), path: path, sync: sync}, nil
}

func (w *wal) append(r rec) error {
	w.buf = encodeRec(w.buf[:0], r, &w.scratch)
	if _, err := w.bw.Write(w.buf); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

// reset, flush sonrası log'u sıfırlar. Silmek yerine O_TRUNC ile
// kesiyoruz: Windows'ta yeni kapatılmış bir dosyayı silmek (antivirüs
// taraması, delete-pending durumu) geçici olarak başarısız olabilir;
// truncate bu tuzağa düşmez.
func (w *wal) reset() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.f, w.bw = f, bufio.NewWriter(f)
	return nil
}

func (w *wal) close() error { return w.f.Close() }

// replayWAL, log'daki kayıtları sırayla fn'e verir. Yarım yazılmış
// kuyruk (torn tail: crash anında kesilen son kayıt) sessizce atılır —
// o kayıt zaten hiç "tamamlanmamış" bir yazmadır. Log ortasındaki CRC
// bozukluğu da aynı şekilde durdurur; kuyruktan ayırt edilemez
// (RocksDB'nin varsayılan davranışıyla aynı; kalan kayıtlar feda edilir).
func replayWAL(path string, fn func(rec)) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	for {
		r, err := decodeRec(br)
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrCorrupt) {
			return nil // torn tail: buraya kadarki kayıtlar geçerli
		}
		if err != nil {
			return err
		}
		fn(r)
	}
}
