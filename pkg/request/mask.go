package request

import (
	"bytes"
	"encoding/binary"

	"github.com/bits-and-blooms/bitset"
)

type NullMask struct {
	bs bitset.BitSet
}

func (n *NullMask) set(idx uint) *NullMask {
	n.bs.Set(idx)
	return n
}

func (n *NullMask) shrink(bSize int) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, n.bs.Bytes())
	if err != nil {
		return nil, err
	}
	return buf.Bytes()[:bSize], nil
}
