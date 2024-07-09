package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewImmemtable(t *testing.T) {
	tree := NewTree("1")
	imm := NewImmemtable(tree)
	assert.NotNil(t, imm)
}
