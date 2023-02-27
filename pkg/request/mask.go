package request

import (
	"bytes"
	"encoding/binary"

	"github.com/bits-and-blooms/bitset"
)

type Mask struct {
	bs bitset.BitSet
}

func (n *Mask) set(idx uint) *Mask {
	n.bs.Set(idx)
	return n
}

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
