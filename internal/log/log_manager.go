package log

import (
	"fmt"
	"kura/internal/file"
)

type LogManager struct {
	fileManager    *file.FileManager
	logFile        string
	logPage        *file.Page
	currentBlockID *file.BlockID
	latestLSN      int // latest log sequence number
	lastSavedLSN   int // last saved log sequence number
}

func NewLogManager(fileManager *file.FileManager, logFile string) (*LogManager, error) {
	logSize, err := fileManager.Length(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get log file length %s: %w", logFile, err)
	}
	logPage := file.NewPage(fileManager.BlockSize())

	var currentBlockID *file.BlockID
	if logSize == 0 {
		newBlockID, err := appendNewBlock(fileManager, logFile, logPage)
		if err != nil {
			return nil, fmt.Errorf("failed to create new block for empty log file %s: %w", logFile, err)
		}
		currentBlockID = newBlockID
	} else {
		currentBlockID = file.NewBlockID(logFile, logSize-1)
		if err := fileManager.Read(currentBlockID, logPage); err != nil {
			return nil, fmt.Errorf("failed to read the last log block %s: %w", logFile, err)
		}
	}

	return &LogManager{
		fileManager:    fileManager,
		logFile:        logFile,
		logPage:        logPage,
		currentBlockID: currentBlockID,
		latestLSN:      0,
		lastSavedLSN:   0,
	}, nil
}

// Flush forces a log page to write to disk if the LSN exceeds last saved LSN.
func (lm *LogManager) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

func (lm *LogManager) Iterator() *LogIterator {
	lm.Flush(lm.latestLSN)
	return NewLogIterator(lm.fileManager, lm.currentBlockID)
}

// Append adds a new log record and returns the latest LSN.
func (lm *LogManager) Append(logRecord []byte) (int, error) {
	boundary := int(lm.logPage.GetInt(0))
	recordSize := len(logRecord)
	bytesNeeded := recordSize + file.Int32Size

	if boundary-bytesNeeded < file.Int32Size {
		lm.flush() // move to the next block
		newBlockID, err := appendNewBlock(lm.fileManager, lm.logFile, lm.logPage)
		if err != nil {
			return 0, fmt.Errorf("failed to append new block to log file %s: %w", lm.logFile, err)
		}
		lm.currentBlockID = newBlockID
		boundary = int(lm.logPage.GetInt(0))
	}

	recordPos := boundary - bytesNeeded
	lm.logPage.SetBytes(recordPos, logRecord)
	lm.logPage.SetInt(0, int32(recordPos))
	lm.latestLSN += 1
	return lm.latestLSN, nil
}

func appendNewBlock(fileManager *file.FileManager, logFile string, logPage *file.Page) (*file.BlockID, error) {
	blockID, err := fileManager.Append(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to append block to log file %s: %w", logFile, err)
	}
	logPage.SetInt(0, int32(fileManager.BlockSize()))
	if err := fileManager.Write(blockID, logPage); err != nil {
		return nil, fmt.Errorf("failed to write initial block data to log file %s: %w", logFile, err)
	}
	return blockID, nil
}

// flush writes the current log page to disk and updates the last saved LSN.
func (lm *LogManager) flush() {
	if err := lm.fileManager.Write(lm.currentBlockID, lm.logPage); err != nil {
		fmt.Printf("error flushing log page to disk for file %s: %v\n", lm.logFile, err)
		return
	}
	lm.lastSavedLSN = lm.latestLSN
}

func (lm *LogManager) CurrentBlockID() *file.BlockID {
	return lm.currentBlockID
}
