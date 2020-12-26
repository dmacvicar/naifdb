package kv

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type StoreOption func(*Store)

func WithDirectory(dir string) StoreOption {
	return func(s *Store) {
		s.dir = dir
	}
}

func indexDataFile(store *Store, dataFileName string) error {
	log.Printf("Reading data file to index keys: %s\n", dataFileName)
	dataFileF, err := os.Open(path.Join(store.dir, dataFileName))
	if err != nil {
		return err
	}
	store.files = append(store.files, dataFileF)

	// we want to index the keys and locations, avoiding reading the values
	for {
		entryWithKey, err := readKeyDbEntry(dataFileF)
		if err != nil && err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// associate the entry with the current file id
		// insert entry into keyDb
		store.keyDb[string(entryWithKey.Key)] = entryWithKey.keyDbEntry
	}
}

type Store struct {
	dir   string
	mutex sync.Mutex
	// key to offet index
	// TODO []byte can't be used. Convert to string for now
	keyDb keyDb
	// stack of RO log files
	files []*os.File
	// append file
	activeF *os.File
}

func NewStore(opts ...StoreOption) (*Store, error) {
	store := &Store{}
	store.keyDb = make(keyDb)

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

	// they are already sorted by filename, and we use timestamp as filename
	dataFiles, err := ioutil.ReadDir(store.dir)
	if err != nil {
		return nil, err
	}

	for _, dataFile := range dataFiles {
		if !strings.HasSuffix(dataFile.Name(), ".data") {
			log.Printf("Skip: %s", dataFile.Name())
			continue
		}
		indexDataFile(store, dataFile.Name())
	}

	// For now, we always start with a fresh append file and never reopen old RO files
	// even if they were small when the process was restarted
	activeFile := fmt.Sprintf("%d.data", time.Now().UnixNano())
	activeF, err := os.OpenFile(path.Join(store.dir, activeFile), os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	store.activeF = activeF

	return store, nil
}

func (s *Store) Close() {
	log.Println("Closing...")
	for _, file := range s.files {
		file.Close()
	}
	s.activeF.Close()
}

func (s *Store) Set(key []byte, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	offset, err := s.activeF.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	rec := record{}
	rec.Set(key, value)
	n, err := rec.WriteTo(s.activeF)
	if err != nil {
		// if there is an error but something was writen, revert
		if n == 0 {
			return err
		}
		errTrunc := s.activeF.Truncate(offset)
		if errTrunc != nil {
			return errTrunc
		}
		return err
	}

	// no error but not all data was written
	expectedN := int64(4 + 8 + 8 + 8 + len(key) + len(value))
	if n != expectedN {
		// not all data was written
		errTrunc := s.activeF.Truncate(offset)
		if errTrunc != nil {
			return errTrunc
		}
		return fmt.Errorf("Only %d/%d bytes writen", n, expectedN)
	}
	log.Printf("Wrote entry %d bytes", n)

	// no error, all data was written
	idx := keyDbEntry{}
	idx.Timestamp = rec.Timestamp
	idx.fileId = int64(len(s.files))
	idx.ValueSize = rec.ValueSize

	idx.ValueOffset = int64(offset) + 4 + 8 + 8 + 8 + int64(len(key))

	s.keyDb[string(key)] = &idx
	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if key in index
	if entry, ok := s.keyDb[string(key)]; ok {
		// determine the file to use
		var file *os.File
		if entry.fileId < int64(len(s.files)) {
			file = s.files[entry.fileId]
		} else {
			// active file
			file = s.activeF
		}

		if file == nil {
			return nil, fmt.Errorf("No file id %d in index", entry.fileId)
		}
		file.Seek(entry.ValueOffset, io.SeekStart)
		buf := make([]byte, entry.ValueSize)
		n, err := file.Read(buf)
		if err != nil {
			return nil, err
		}
		if int64(n) != entry.ValueSize {
			return nil, fmt.Errorf("Incomplete read %d/%d for: %s", n, entry.ValueSize, string(key))
		}
		return buf, nil
	}

	// Keys not in index are not implemented
	return nil, fmt.Errorf("Keys not in index are not implemented")
}
