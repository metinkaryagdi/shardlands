package storage

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

// maxRecBytes, bozuk bir uzunluk önekinin dev allocation'a (OOM)
// dönüşmesini engeller.
const maxRecBytes = 1 << 28

// Kayıt çerçevesi — WAL kayıtları ve SSTable girdileri aynı biçimi
// paylaşır:
//
//	[payloadLen uvarint][crc32(payload) 4B LE][payload]
//	payload = [keyLen uvarint][key][flags 1B][value]
//
// CRC her kaydı bağımsız doğrulanabilir yapar: WAL'da yarım yazılmış
// kuyruk (torn tail) tespiti, SSTable'da bit çürümesi tespiti buradan
// gelir. flags bit0 = tombstone.

// encodeRec, r'yi dst'ye ekleyip döner. scratch, payload kurulumu için
// çağrılar arasında yeniden kullanılan tampondur (allocation azaltır).
func encodeRec(dst []byte, r rec, scratch *[]byte) []byte {
	p := (*scratch)[:0]
	p = binary.AppendUvarint(p, uint64(len(r.key)))
	p = append(p, r.key...)
	var flags byte
	if r.tomb {
		flags = 1
	}
	p = append(p, flags)
	p = append(p, r.val...)
	*scratch = p

	dst = binary.AppendUvarint(dst, uint64(len(p)))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.ChecksumIEEE(p))
	return append(dst, p...)
}

// decodeRec bir kayıt okur. Temiz akış sonu io.EOF; kayıt ortasında
// kesilme io.ErrUnexpectedEOF; CRC/format bozukluğu ErrCorrupt döner.
// Dönen rec'in key/val dilimleri taze tampona aittir, saklanabilir.
func decodeRec(br *bufio.Reader) (rec, error) {
	plen, err := binary.ReadUvarint(br)
	if err != nil {
		if err == io.EOF {
			return rec{}, io.EOF
		}
		return rec{}, err
	}
	if plen == 0 || plen > maxRecBytes {
		return rec{}, ErrCorrupt
	}
	var crcb [4]byte
	if _, err := io.ReadFull(br, crcb[:]); err != nil {
		return rec{}, asTornTail(err)
	}
	payload := make([]byte, plen)
	if _, err := io.ReadFull(br, payload); err != nil {
		return rec{}, asTornTail(err)
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(crcb[:]) {
		return rec{}, ErrCorrupt
	}

	klen, n := binary.Uvarint(payload)
	if n <= 0 || uint64(n)+klen+1 > uint64(len(payload)) {
		return rec{}, ErrCorrupt
	}
	key := payload[uint64(n) : uint64(n)+klen]
	flags := payload[uint64(n)+klen]
	val := payload[uint64(n)+klen+1:]
	return rec{key: key, val: val, tomb: flags&1 == 1}, nil
}

// asTornTail: kayıt ortasında biten akış her zaman ErrUnexpectedEOF
// olarak sınıflansın ki WAL replay "temiz son" ile "yarım kayıt"ı
// ayırt edebilsin.
func asTornTail(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
