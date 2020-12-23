package kv

import (
	"bytes"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"log"
	"os"
	"sync"
	"path"
)

const (
	DbFilename string = "log.db"
)

type StoreOption func(*Store)

func WithDirectory(dir string) StoreOption {
	return func(s *Store) {
		s.dir = dir
	}
}

type Record struct {
	Key   []byte
	Value []byte
}

type Store struct {
	dir string
	mutex sync.Mutex
	// key to offet index
	// TODO []byte can't be used. Convert to string for now
	index    map[string]int64
	fileDesc *os.File
	// current file offset
	offset int64
	// how many iterations matching the last key
	Iterations int
}

func NewStore(opts ...StoreOption) (*Store, error) {
	store := &Store{}

	var err error
	store.dir, err = os.Getwd()
	if err != nil {
		return nil, err
	}

	// apply all passed options
	for _, opt := range opts {
		opt(store)
	}

	// make sure the data dir exists
	if err := os.MkdirAll(store.dir, 0755); err != nil {
		return nil, err
	}

	store.fileDesc, err = os.OpenFile(path.Join(store.dir, DbFilename), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// calculate current offset
	stat, err := store.fileDesc.Stat()
	if err != nil {
		return nil, err
	}
	store.offset = stat.Size()
	log.Println("Current offset:", store.offset)

	store.index = make(map[string]int64)
	// TODO read the whole index into memory
    return store, nil
}

func (s *Store) Close() {
	log.Println("Closing...")
	s.fileDesc.Close()
}

func (s *Store) Set(key []byte, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	b, err := msgpack.Marshal(&Record{Key: key, Value: value})
	if err != nil {
		return err
	}
	n, err := s.fileDesc.Write(b)
	if err != nil {
		// if there is an error but something was writen, revert
		if n == 0 {
			return err
		}
		errTrunc := s.fileDesc.Truncate(s.offset)
		if errTrunc != nil {
			return errTrunc
		}
		return err
	}

	// no error but not all data was written
	if n != len(b) {
		// not all data was written
		errTrunc := s.fileDesc.Truncate(s.offset)
		if errTrunc != nil {
			return errTrunc
		}
		return fmt.Errorf("Only %d/%d bytes writen", n, len(b))
	}

	// no error, all data was written
	s.index[string(key)] = s.offset
	s.offset = s.offset + int64(n)
	log.Printf("Wrote entry %d bytes", n)
	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.fileDesc.Seek(0, 0)
	decoder := msgpack.NewDecoder(s.fileDesc)
	its := 0
	defer func() {
		s.Iterations = its
		log.Printf("Scanned %d records", its)
	}()
	for {
		record := Record{}
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if bytes.Compare(record.Key, key) == 0 {
			return record.Value, nil
		}
		its = its + 1
	}
	// TODO should be error NotFound?
	return nil, nil
}
