package server

import (
	"fmt"

	"kura/internal/file"
)

type KuraDB struct {
	fileManager *file.FileManager
}

func NewKuraDB(dbDir string, blockSize int) (*KuraDB, error) {
	fileManager, err := file.NewFileManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	return &KuraDB{
		fileManager: fileManager,
	}, nil
}

func (db *KuraDB) FileManager() *file.FileManager {
	return db.fileManager
}
