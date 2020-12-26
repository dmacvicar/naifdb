package kv

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

// Creates a zero value for a database record
type record struct {
	CRC32     uint32
	Timestamp int64
	KeySize   int64
	ValueSize int64
	Key       []byte
	Value     []byte
}

// Fills the record with a key/value pair and calculating the associated
// metadata like sizes, timestamp and checksums
func (rec *record) Set(key []byte, value []byte) {
	rec.CRC32 = crc32.Checksum(value, crc32.IEEETable)
	rec.Timestamp = time.Now().Unix()
	rec.KeySize = int64(len(key))
	rec.ValueSize = int64(len(value))
	rec.Key = key
	rec.Value = value
}

// Writes a database record to a writer
// Returns the number of bytes written and any encountered error
func (rec *record) WriteTo(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)
	data := []interface{}{&rec.CRC32, &rec.Timestamp, &rec.KeySize, &rec.ValueSize}
	for _, item := range data {
		err := binary.Write(buf, binary.LittleEndian, item)
		if err != nil {
			return 0, err
		}
	}
	// binary.Write only support fixed length values. We only use it to encode
	// integers and write byte slices directly
	buf.Write(rec.Key)
	buf.Write(rec.Value)

	return buf.WriteTo(w)
}
