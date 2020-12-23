package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStore(t *testing.T) {

	pairs := [][]string{
		{"Key1", "Value 1"},
		{"Key2", "Value 2"},
		{"Key3", "Value 3"},
	}

	store, err := NewStore()
	defer store.Close()

	for _, pair := range pairs {
		err = store.Set([]byte(pair[0]), []byte(pair[1]))
		assert.NoError(t, err)
	}

	for _, pair := range pairs {
		value, err := store.Get([]byte(pair[0]))
		assert.NoError(t, err)
		assert.Equal(t, pair[1], string(value))
	}
}
