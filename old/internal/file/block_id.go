package file

import (
	"fmt"
	"hash/fnv"
)

type BlockID struct {
	filename string
	blockNum int64
}

func NewBlockID(filename string, blockNum int64) *BlockID {
	return &BlockID{
		filename: filename,
		blockNum: blockNum,
	}
}

func (b *BlockID) Filename() string {
	return b.filename
}

func (b *BlockID) Number() int64 {
	return b.blockNum
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blockNum)
}

func (b *BlockID) Hash() uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(b.String()))
	return h.Sum32()
}
