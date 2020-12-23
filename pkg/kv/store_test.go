package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"path"
)

func TestStore(t *testing.T) {

	pairs := [][]string{
		{"Key1", "Value 1"},
		{"Key2", "Value 2"},
		{"Key3", "Value 3"},
	}

	datadir := t.TempDir()

	store, err := NewStore(WithDirectory(datadir))
	defer func() {
		store.Close()
		assert.FileExists(t, path.Join(datadir, "log.db"))
	}()

	for _, pair := range pairs {
		err = store.Set([]byte(pair[0]), []byte(pair[1]))
		assert.NoError(t, err)
	}

	for _, pair := range pairs {
		value, err := store.Get([]byte(pair[0]))
		assert.NoError(t, err)
		assert.Equal(t, pair[1], string(value))
	}

	assert.Equal(t, 1, store.Iterations, "Finding the last key should take a single iteration")

}
