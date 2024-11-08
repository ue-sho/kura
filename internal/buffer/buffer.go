package buffer

import (
	"kura/internal/file"
	"kura/internal/log"
)

type Buffer struct {
	fileManager *file.FileManager
	logManager  *log.LogManager
	contents    *file.Page
	blockID     *file.BlockID
	pins        int
	txNum       int
	lsn         int
}

// NewBuffer creates a new Buffer with the given FileMgr and LogMgr
func NewBuffer(fileManager *file.FileManager, logManager *log.LogManager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		contents:    file.NewPage(fileManager.BlockSize()),
		blockID:     nil,
		pins:        0,
		txNum:       -1,
		lsn:         -1,
	}
}

// Contents returns the contents of the buffer (the page).
func (b *Buffer) Contents() *file.Page {
	return b.contents
}

// Block returns the BlockID associated with this buffer.
func (b *Buffer) Block() *file.BlockID {
	return b.blockID
}

// SetModified marks the buffer as modified by a transaction.
func (b *Buffer) SetModified(txNum, lsn int) {
	b.txNum = txNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// IsPinned checks if the buffer is currently pinned.
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

// ModifyingTx returns the transaction ID that modified the buffer.
func (b *Buffer) ModifyingTx() int {
	return b.txNum
}

// AssignToBlock assigns a block to the buffer and reads its contents.
func (b *Buffer) AssignToBlock(block *file.BlockID) error {
	if err := b.Flush(); err != nil {
		return err
	}
	b.blockID = block
	b.pins = 0
	return b.fileManager.Read(block, b.contents)
}

// Flush writes the contents of the buffer to disk if it has been modified.
func (b *Buffer) Flush() error {
	if b.txNum >= 0 {
		b.logManager.Flush(b.lsn)
		if err := b.fileManager.Write(b.blockID, b.contents); err != nil {
			return err
		}
		b.txNum = -1
	}
	return nil
}

// Pin increments the pin count for the buffer.
func (b *Buffer) Pin() {
	b.pins++
}

// Unpin decrements the pin count for the buffer.
func (b *Buffer) Unpin() {
	b.pins--
}
