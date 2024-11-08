package buffer

import (
	"fmt"
	"kura/internal/file"
	"kura/internal/log"
	"time"
)

type BufferManager struct {
	bufferpool   []*Buffer
	numAvailable int
	maxTime      time.Duration
}

// NewBufferManager initializes the buffer pool with the given number of buffers.
func NewBufferManager(fileManager *file.FileManager, logManager *log.LogManager, numBuffs int) *BufferManager {
	bufferManager := &BufferManager{
		bufferpool:   make([]*Buffer, numBuffs),
		numAvailable: numBuffs,
		maxTime:      10 * time.Second, // 10 seconds
	}
	for i := 0; i < numBuffs; i++ {
		bufferManager.bufferpool[i] = NewBuffer(fileManager, logManager)
	}
	return bufferManager
}

// Available returns the number of available buffers.
func (bm *BufferManager) Available() int {
	return bm.numAvailable
}

// FlushAll flushes all buffers modified by the specified transaction ID.
func (bm *BufferManager) FlushAll(txnum int) {
	for _, buff := range bm.bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.Flush()
		}
	}
}

// Unpin unpins the specified buffer and notifies waiting clients if the buffer becomes available.
func (bm *BufferManager) Unpin(buff *Buffer) {
	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
	}
}

// Pin tries to pin a block and waits if necessary until an available buffer is found.
func (bm *BufferManager) Pin(blk *file.BlockID) (*Buffer, error) {
	startTime := time.Now()
	buff := bm.tryToPin(blk)
	for buff == nil && !bm.waitingTooLong(startTime) {
		buff = bm.tryToPin(blk)
	}
	if buff == nil {
		return nil, fmt.Errorf("BufferAbortException: no available buffers")
	}
	return buff, nil
}

func (bm *BufferManager) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > bm.maxTime
}

func (bm *BufferManager) tryToPin(blk *file.BlockID) *Buffer {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blk)
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff
}

func (bm *BufferManager) findExistingBuffer(blockID *file.BlockID) *Buffer {
	for _, buff := range bm.bufferpool {
		if buff.Block() != nil && buff.blockID == blockID {
			return buff
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferpool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
