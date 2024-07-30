package bloom_filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	bf := New()
	bf.Insert([]byte("1"))
	bf.Insert([]byte("2"))
	bf.Insert([]byte("3"))

	assert.Equal(t, true, bf.MayContain([]byte("3")))
	assert.Equal(t, true, bf.MayContain([]byte("2")))
	assert.Equal(t, true, bf.MayContain([]byte("1")))

	assert.Equal(t, false, bf.MayContain([]byte("10")))
	assert.Equal(t, false, bf.MayContain([]byte("11")))
	assert.Equal(t, false, bf.MayContain([]byte("13")))
	assert.Equal(t, false, bf.MayContain([]byte("12")))
}
