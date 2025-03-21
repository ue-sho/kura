package buffer

import (
	"kura/internal/file"
	"kura/internal/log"
)

type Buffer struct {
	fileManager *file.FileManager
	logManager  *log.LogManager
	contents    *file.Page
	block       *file.BlockID
	pins        int
	txNum       int
	lsn         int
}

func NewBuffer(fileManager *file.FileManager, logManager *log.LogManager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		contents:    file.NewPage(fileManager.BlockSize()),
		block:       nil,
		pins:        0,
		txNum:       -1,
		lsn:         -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.BlockID {
	return b.block
}

func (b *Buffer) SetModified(txNum, lsn int) {
	b.txNum = txNum
	// a negative LSN indicates that a log record was not generated for that update
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int {
	return b.txNum
}

func (b *Buffer) AssignToBlock(block *file.BlockID) error {
	if err := b.Flush(); err != nil {
		return err
	}
	b.block = block
	b.pins = 0
	return b.fileManager.Read(block, b.contents)
}

func (b *Buffer) Flush() error {
	if b.txNum > 0 {
		return nil
	}

	b.logManager.Flush(b.lsn)
	if b.block == nil {
		return nil
	}
	if err := b.fileManager.Write(b.block, b.contents); err != nil {
		return err
	}
	b.txNum = -1
	return nil
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}
