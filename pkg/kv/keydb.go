package kv

import (
	"encoding/binary"
	"io"
)

// An entry in the key index
type keyDbEntry struct {
	fileId      int64
	ValueSize   int64
	ValueOffset int64
	Timestamp   int64
}

// the type for the key index
type keyDb map[string]*keyDbEntry

// helper for returning an entry plus its associated key
type keyDbEntryWithKey struct {
	*keyDbEntry
	Key []byte
}

// returns key, entry and err
func readKeyDbEntry(r io.ReadSeeker) (*keyDbEntryWithKey, error) {
	entry := keyDbEntry{}
	// skip crc
	_, err := r.Seek(4, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.LittleEndian, &entry.Timestamp)
	if err != nil {
		return nil, err
	}
	var keysize int64
	err = binary.Read(r, binary.LittleEndian, &keysize)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.LittleEndian, &entry.ValueSize)
	if err != nil {
		return nil, err
	}
	key := make([]byte, keysize)
	err = binary.Read(r, binary.LittleEndian, &key)
	if err != nil {
		return nil, err
	}

	offset, err := r.Seek(0, io.SeekCurrent)
	// skip value
	_, err = r.Seek(entry.ValueSize, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	entry.ValueOffset = offset
	return &keyDbEntryWithKey{keyDbEntry: &entry, Key: key}, nil
}
