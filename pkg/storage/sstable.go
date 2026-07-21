package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// SSTable (Sorted String Table): anahtar sırasına göre yazılmış,
// DEĞİŞMEZ dosya. Değişmezlik LSM'nin kalbi: dosya bir kez yazılıp
// sync'lendikten sonra asla güncellenmez — eşzamanlı okuma kilitsiz,
// crash yarısı yazılmış dosyayı en fazla "çöp" yapar (manifest'e
// girmediği için görünmez), asla mevcut veriyi bozmaz.
//
// Dosya düzeni:
//
//	[kayıtlar...]                 codec.go çerçevesiyle, artan anahtar
//	[sparse index]                her indexInterval kayıtta bir: anahtar → ofset
//	[bloom filter]
//	[footer 32B]                  indexOff | bloomOff | count | magic (uint64 LE)
//
// Sparse index tüm anahtarları değil her 16.'yı tutar: Get, binary
// search ile doğru "dilimi" bulur, en çok 16 kayıt tarar. RAM maliyeti
// anahtar başına ~1/16'ya iner (LevelDB'nin blok index'inin sadeleşmiş
// hali).
const (
	indexInterval = 16
	tableMagic    = 0x53484152_44420001 // "SHARDB" v1
	footerLen     = 32
)

type indexEntry struct {
	key []byte
	off uint64
}

// ---- yazma ----

type tableBuilder struct {
	bw      *bufio.Writer
	off     uint64
	index   []indexEntry
	hashes  []uint64
	n       uint64
	lastKey []byte
	buf     []byte // kayıt çerçevesi tamponu
	scratch []byte // codec payload tamponu
}

func (b *tableBuilder) add(r rec) error {
	if b.lastKey != nil && bytes.Compare(r.key, b.lastKey) <= 0 {
		return fmt.Errorf("%w: sstable keys must be strictly ascending", ErrCorrupt)
	}
	if b.n%indexInterval == 0 {
		b.index = append(b.index, indexEntry{key: clone(r.key), off: b.off})
	}
	b.buf = encodeRec(b.buf[:0], r, &b.scratch)
	if _, err := b.bw.Write(b.buf); err != nil {
		return err
	}
	b.off += uint64(len(b.buf))
	b.hashes = append(b.hashes, bloomHash(r.key))
	b.n++
	b.lastKey = append(b.lastKey[:0], r.key...)
	return nil
}

func (b *tableBuilder) finish(bitsPerKey int) error {
	indexOff := b.off
	out := binary.AppendUvarint(nil, uint64(len(b.index)))
	for _, e := range b.index {
		out = binary.AppendUvarint(out, uint64(len(e.key)))
		out = append(out, e.key...)
		out = binary.AppendUvarint(out, e.off)
	}
	bloomOff := indexOff + uint64(len(out))
	out = newBloom(b.hashes, bitsPerKey).marshal(out)

	var footer [footerLen]byte
	binary.LittleEndian.PutUint64(footer[0:], indexOff)
	binary.LittleEndian.PutUint64(footer[8:], bloomOff)
	binary.LittleEndian.PutUint64(footer[16:], b.n)
	binary.LittleEndian.PutUint64(footer[24:], tableMagic)
	out = append(out, footer[:]...)

	if _, err := b.bw.Write(out); err != nil {
		return err
	}
	return b.bw.Flush()
}

// writeTable, it'in akıttığı (artan anahtarlı) kayıtları path'e kalıcı
// bir SSTable olarak yazar ve fsync'ler.
func writeTable(path string, it iterator, bitsPerKey int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	b := &tableBuilder{bw: bufio.NewWriter(f)}
	for {
		r, ok, err := it.next()
		if err != nil {
			f.Close()
			return err
		}
		if !ok {
			break
		}
		if err := b.add(r); err != nil {
			f.Close()
			return err
		}
	}
	if err := b.finish(bitsPerKey); err != nil {
		f.Close()
		return err
	}
	// fsync olmadan "dosya yazıldı" demek OS cache'ine güvenmektir;
	// manifest bu dosyayı işaret etmeden önce içerik diske inmiş olmalı.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ---- okuma ----

type sstable struct {
	f        *os.File
	path     string
	indexOff int64
	count    int
	index    []indexEntry
	filter   *bloom
}

func openTable(path string) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := st.Size()
	if size < footerLen {
		f.Close()
		return nil, fmt.Errorf("%w: %s too small", ErrCorrupt, path)
	}
	var footer [footerLen]byte
	if _, err := f.ReadAt(footer[:], size-footerLen); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[24:]) != tableMagic {
		f.Close()
		return nil, fmt.Errorf("%w: %s bad magic", ErrCorrupt, path)
	}
	indexOff := int64(binary.LittleEndian.Uint64(footer[0:]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[8:]))
	count := int64(binary.LittleEndian.Uint64(footer[16:]))
	if indexOff < 0 || bloomOff < indexOff || bloomOff > size-footerLen {
		f.Close()
		return nil, fmt.Errorf("%w: %s bad section offsets", ErrCorrupt, path)
	}

	t := &sstable{f: f, path: path, indexOff: indexOff, count: int(count)}
	if err := t.loadIndex(indexOff, bloomOff); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := t.loadBloom(bloomOff, size-footerLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

func (t *sstable) loadIndex(from, to int64) error {
	buf := make([]byte, to-from)
	if _, err := t.f.ReadAt(buf, from); err != nil {
		return err
	}
	cnt, n := binary.Uvarint(buf)
	if n <= 0 {
		return ErrCorrupt
	}
	buf = buf[n:]
	t.index = make([]indexEntry, 0, cnt)
	for i := uint64(0); i < cnt; i++ {
		klen, n := binary.Uvarint(buf)
		if n <= 0 || uint64(len(buf[n:])) < klen {
			return ErrCorrupt
		}
		key := buf[n : uint64(n)+klen]
		buf = buf[uint64(n)+klen:]
		off, n2 := binary.Uvarint(buf)
		if n2 <= 0 {
			return ErrCorrupt
		}
		buf = buf[n2:]
		t.index = append(t.index, indexEntry{key: key, off: off})
	}
	return nil
}

func (t *sstable) loadBloom(from, to int64) error {
	buf := make([]byte, to-from)
	if _, err := t.f.ReadAt(buf, from); err != nil {
		return err
	}
	b, err := unmarshalBloom(buf)
	if err != nil {
		return err
	}
	t.filter = b
	return nil
}

// get: bloom → sparse index binary search → en çok bir dilim (≤16
// kayıt) sıralı tarama.
func (t *sstable) get(key []byte) (rec, bool, error) {
	if !t.filter.mayContain(bloomHash(key)) {
		return rec{}, false, nil
	}
	// İlk "index anahtarı > key" konumunun bir öncesi = key'i
	// içerebilecek dilim.
	i := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].key, key) > 0
	})
	if i == 0 {
		return rec{}, false, nil // key, tablodaki en küçük anahtardan küçük
	}
	off := int64(t.index[i-1].off)
	br := bufio.NewReader(io.NewSectionReader(t.f, off, t.indexOff-off))
	for {
		r, err := decodeRec(br)
		if err == io.EOF {
			return rec{}, false, nil
		}
		if err != nil {
			return rec{}, false, fmt.Errorf("%s: %w", t.path, err)
		}
		switch c := bytes.Compare(r.key, key); {
		case c == 0:
			return r, true, nil
		case c > 0:
			return rec{}, false, nil // sıralı dosyada geçtiysek yoktur
		}
	}
}

type tableIter struct {
	br   *bufio.Reader
	path string
	from []byte // nil değilse: bu anahtardan küçükleri atla
}

func (t *sstable) iter() iterator { return t.iterFrom(nil) }

// iterFrom: sparse index ile from'u içerebilecek dilime atlar, dilim
// içinde from'dan küçük kayıtları akış sırasında atlar. Veri bölümü
// tam indexOff'ta bittiği için akış sonu io.EOF ile belirlenir.
func (t *sstable) iterFrom(from []byte) iterator {
	off := int64(0)
	if len(from) > 0 {
		i := sort.Search(len(t.index), func(i int) bool {
			return bytes.Compare(t.index[i].key, from) > 0
		})
		if i > 0 {
			off = int64(t.index[i-1].off)
		}
	}
	return &tableIter{
		br:   bufio.NewReader(io.NewSectionReader(t.f, off, t.indexOff-off)),
		path: t.path,
		from: from,
	}
}

func (it *tableIter) next() (rec, bool, error) {
	for {
		r, err := decodeRec(it.br)
		if err == io.EOF {
			return rec{}, false, nil
		}
		if err != nil {
			return rec{}, false, fmt.Errorf("%s: %w", it.path, err)
		}
		if it.from != nil {
			if bytes.Compare(r.key, it.from) < 0 {
				continue
			}
			it.from = nil // eşiği geçtik; artık karşılaştırma yok
		}
		return r, true, nil
	}
}

func (t *sstable) close() error { return t.f.Close() }
