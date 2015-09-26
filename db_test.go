package rdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDB DB

func init() {
	testDB = NewDB("./test")
}

func TestMetaSet(t *testing.T) {
	assert.NoError(t, testDB.Set("foo", "bar"))

	v, err := testDB.Get("foo")
	require.NoError(t, err)
	assert.Equal(t, "bar", v)
}

func BenchmarkSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testDB.Set("foo", "bar")
	}
}
