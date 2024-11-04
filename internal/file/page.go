package file

import (
	"encoding/binary"
)

type Page struct {
	byteBuffer []byte
}

const int32Size = 4 // int32 is 4 bytes

func NewPage(blockSize int64) *Page {
	return &Page{
		byteBuffer: make([]byte, blockSize),
	}
}

func NewPageFromBytes(b []byte) *Page {
	return &Page{
		byteBuffer: b,
	}
}

func (p *Page) GetInt(offset int) int32 {
	return int32(binary.LittleEndian.Uint32(p.byteBuffer[offset : offset+int32Size]))
}

func (p *Page) SetInt(offset int, val int32) {
	binary.LittleEndian.PutUint32(p.byteBuffer[offset:offset+int32Size], uint32(val))
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.byteBuffer[offset+int32Size : offset+int32Size+int(length)]
}

func (p *Page) SetBytes(offset int, val []byte) {
	p.SetInt(offset, int32(len(val)))
	copy(p.byteBuffer[offset+int32Size:], val)
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, val string) {
	p.SetBytes(offset, []byte(val))
}

func MaxLength(strlen int) int {
	return int32Size + strlen
}

func (p *Page) contents() []byte {
	return p.byteBuffer
}
