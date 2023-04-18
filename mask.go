package greptime

import (
	"bytes"
	"encoding/binary"

	"github.com/bits-and-blooms/bitset"
)

// Mask is help to set null bits.
type Mask struct {
	bs bitset.BitSet
}

// set is to set which position is to be set to 1
func (n *Mask) set(idx uint) *Mask {
	n.bs.Set(idx)
	return n
}

// shrink is to help to generate the bytes number the caller is interested
// via LittleEndian
func (n *Mask) shrink(bSize int) ([]byte, error) {
	if n.bs.Len() == 0 {
		return nil, nil
	}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, n.bs.Bytes())
	if err != nil {
		return nil, err
	}
	return buf.Bytes()[:bSize], nil
}
