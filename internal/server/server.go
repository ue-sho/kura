package server

import (
	"fmt"

	"kura/internal/file"
	"kura/internal/log"
)

type KuraDB struct {
	fileManager *file.FileManager
	logManager  *log.LogManager
}

func NewKuraDB(dbDir string, blockSize int, bufferSize int) (*KuraDB, error) {
	fileManager, err := file.NewFileManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	logManager, err := log.NewLogManager(fileManager, "kura.log")
	if err != nil {
		return nil, fmt.Errorf("log.NewLogManager: %w", err)
	}

	return &KuraDB{
		fileManager: fileManager,
		logManager:  logManager,
	}, nil
}

func (db *KuraDB) FileManager() *file.FileManager {
	return db.fileManager
}

func (db *KuraDB) LogManager() *log.LogManager {
	return db.logManager
}
