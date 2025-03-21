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

func NewBufferManager(fileManager *file.FileManager, logManager *log.LogManager, numBuffs int) *BufferManager {
	bufferManager := &BufferManager{
		bufferpool:   make([]*Buffer, numBuffs),
		numAvailable: numBuffs,
		maxTime:      10 * time.Second,
	}
	for i := 0; i < numBuffs; i++ {
		bufferManager.bufferpool[i] = NewBuffer(fileManager, logManager)
	}
	return bufferManager
}

func (bm *BufferManager) Available() int {
	return bm.numAvailable
}

func (bm *BufferManager) FlushAll(txNum int) {
	for _, buff := range bm.bufferpool {
		if buff.ModifyingTx() == txNum {
			buff.Flush()
		}
	}
}

func (bm *BufferManager) Unpin(buff *Buffer) {
	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		// notifyAll();
	}
}

func (bm *BufferManager) Pin(block *file.BlockID) (*Buffer, error) {
	startTime := time.Now()
	buff := bm.tryToPin(block)
	for buff == nil && !bm.waitingTooLong(startTime) {
		buff = bm.tryToPin(block)
	}
	if buff == nil {
		return nil, fmt.Errorf("BufferAbortException: no available buffers")
	}
	return buff, nil
}

func (bm *BufferManager) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > bm.maxTime
}

func (bm *BufferManager) tryToPin(block *file.BlockID) *Buffer {
	buff := bm.findExistingBuffer(block)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(block)
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff
}

func (bm *BufferManager) findExistingBuffer(block *file.BlockID) *Buffer {
	for _, buff := range bm.bufferpool {
		if buff.Block() != nil && buff.block == block {
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
