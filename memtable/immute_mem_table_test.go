package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewImmemtable(t *testing.T) {
	tree := NewMemtable("1")
	imm := NewImmemtable(tree)
	assert.NotNil(t, imm)
}
