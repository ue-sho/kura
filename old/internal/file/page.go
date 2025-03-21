package file

import (
	"encoding/binary"
	"time"
)

type Page struct {
	byteBuffer []byte
}

const (
	Int32Size = 4 // int32 is 4 bytes
	Int16Size = 2 // int16 is 2 bytes
	BoolSize  = 1 // boolean is 1 byte
	DateSize  = 8 // date represented as int64 (8 bytes for Unix timestamp)
)

// NewPage creates a new page with the specified block size
func NewPage(blockSize int64) *Page {
	return &Page{
		byteBuffer: make([]byte, blockSize),
	}
}

// NewPageFromBytes creates a page from an existing byte array
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		byteBuffer: b,
	}
}

func (p *Page) GetInt(offset int) int32 {
	return int32(binary.LittleEndian.Uint32(p.byteBuffer[offset : offset+Int32Size]))
}

func (p *Page) SetInt(offset int, val int32) {
	// Performance: Avoids overhead by skipping size checks on each write in high-frequency DB access
	// Caller responsibility: Client code should ensure data fits, improving system efficiency
	// if len(p.byteBuffer) < offset+Int32Size {
	// 	return
	// }
	binary.LittleEndian.PutUint32(p.byteBuffer[offset:offset+Int32Size], uint32(val))
}

func (p *Page) GetShort(offset int) int16 {
	return int16(binary.LittleEndian.Uint16(p.byteBuffer[offset : offset+Int16Size]))
}

func (p *Page) SetShort(offset int, val int16) {
	binary.LittleEndian.PutUint16(p.byteBuffer[offset:offset+Int16Size], uint16(val))
}

func (p *Page) GetBool(offset int) bool {
	return p.byteBuffer[offset] != 0
}

func (p *Page) SetBool(offset int, val bool) {
	if val {
		p.byteBuffer[offset] = 1
	} else {
		p.byteBuffer[offset] = 0
	}
}

func (p *Page) GetDate(offset int) time.Time {
	unixTime := int64(binary.LittleEndian.Uint64(p.byteBuffer[offset : offset+DateSize]))
	return time.Unix(unixTime, 0)
}

func (p *Page) SetDate(offset int, date time.Time) {
	binary.LittleEndian.PutUint64(p.byteBuffer[offset:offset+DateSize], uint64(date.Unix()))
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.byteBuffer[offset+Int32Size : offset+Int32Size+int(length)]
}

func (p *Page) SetBytes(offset int, val []byte) {
	p.SetInt(offset, int32(len(val)))
	copy(p.byteBuffer[offset+Int32Size:], val)
}

func (p *Page) GetString(offset int) string {
	var strBytes []byte
	for i := offset; i < len(p.byteBuffer); i++ {
		if p.byteBuffer[i] == 0 { // Null terminator found
			break
		}
		strBytes = append(strBytes, p.byteBuffer[i])
	}
	return string(strBytes)
}

func (p *Page) SetString(offset int, val string) {
	copy(p.byteBuffer[offset:], []byte(val))
	p.byteBuffer[offset+len(val)] = 0 // Append null terminator
}

// MaxLength calculates the maximum number of bytes needed to store a string of a given length
func MaxLength(strlen int) int {
	return strlen + 1 // Extra byte for the null terminator
}

func (p *Page) contents() []byte {
	return p.byteBuffer
}
