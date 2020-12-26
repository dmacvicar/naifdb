package kv

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestStore(t *testing.T) {

	pairs := [][]string{
		{"Key1", "Value 1"},
		{"Key2", "Value 2"},
		{"Key3", "Value 3"},
	}

	datadir := t.TempDir()

	store, err := NewStore(WithDirectory(datadir))
	assert.NoError(t, err)
	assert.NotNil(t, store)

	for _, pair := range pairs {
		err = store.Set([]byte(pair[0]), []byte(pair[1]))
		assert.NoError(t, err)
	}

	for _, pair := range pairs {
		value, err := store.Get([]byte(pair[0]))
		assert.NoError(t, err)
		assert.Equal(t, pair[1], string(value))
	}

	store.Close()

	dataFiles, err := ioutil.ReadDir(datadir)
	assert.NoError(t, err)
	assert.Len(t, dataFiles, 1, "Should have created one file")

	store2, err := NewStore(WithDirectory(datadir))
	assert.NoError(t, err)
	assert.NotNil(t, store2)

	newPairs := [][]string{
		{"Key1", "New Value 1"},
		{"Key3", "New Value 3"},
	}

	for _, pair := range newPairs {
		err = store2.Set([]byte(pair[0]), []byte(pair[1]))
		assert.NoError(t, err)
	}

	for _, pair := range newPairs {
		value, err := store2.Get([]byte(pair[0]))
		assert.NoError(t, err)
		assert.Equal(t, pair[1], string(value))
	}
	// Key 2 did not change
	value, err := store2.Get([]byte(pairs[1][0]))
	assert.NoError(t, err)
	assert.Equal(t, pairs[1][1], string(value))

	store2.Close()
	dataFiles, err = ioutil.ReadDir(datadir)
	assert.NoError(t, err)
	assert.Len(t, dataFiles, 2, "Should have two files. RO and new append.")

}
