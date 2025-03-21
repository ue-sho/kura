package file

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

type FileManager struct {
	dbDir     string
	blockSize int64
	isNew     bool
	openFiles map[string]*os.File
}

func NewFileManager(dbDir string, blockSize int64) (*FileManager, error) {
	isNew := false

	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		isNew = true
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %v", err)
		}
	}

	files, err := os.ReadDir(dbDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}

	// remove any leftover temporary tables
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "temp") {
			continue
		}

		if err = os.Remove(file.Name()); err != nil {
			return nil, fmt.Errorf("os.Remove: %w", err)
		}
	}

	return &FileManager{
		dbDir:     dbDir,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: make(map[string]*os.File),
	}, nil
}

func (fm *FileManager) Read(blk *BlockID, p *Page) error {
	f, err := fm.getFile(blk.filename)
	if err != nil {
		return fmt.Errorf("cannot read block: %v", err)
	}

	offset := blk.blockNum * fm.blockSize
	_, err = f.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("cannot seek to position %d: %v", offset, err)
	}

	_, err = f.Read(p.contents())
	if err != nil {
		return fmt.Errorf("cannot read block %v: %v", blk, err)
	}

	return nil
}

func (fm *FileManager) Write(blk *BlockID, p *Page) error {
	f, err := fm.getFile(blk.filename)
	if err != nil {
		return fmt.Errorf("cannot write block: %v", err)
	}

	offset := blk.blockNum * fm.blockSize
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to position %d: %v", offset, err)
	}

	if _, err := f.Write(p.contents()); err != nil {
		return fmt.Errorf("cannot write block %v: %v", blk, err)
	}
	return nil
}

func (fm *FileManager) Append(filename string) (*BlockID, error) {
	newBlkNum, err := fm.Length(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot append block: %v", err)
	}

	blk := &BlockID{filename, newBlkNum}
	b := make([]byte, fm.blockSize)

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot append block: %v", err)
	}
	defer f.Seek(0, io.SeekStart)

	offset := blk.blockNum * fm.blockSize
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cannot seek to position %d: %v", offset, err)
	}

	if _, err := f.Write(b); err != nil {
		return nil, fmt.Errorf("cannot write block %v: %v", blk, err)
	}
	return blk, nil
}

func (fm *FileManager) Length(filename string) (int64, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("cannot access file %s: %v", filename, err)
	}

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot get file info for %s: %v", filename, err)
	}

	return info.Size() / fm.blockSize, nil
}

func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

func (fm *FileManager) BlockSize() int64 {
	return fm.blockSize
}

func (fm *FileManager) getFile(filename string) (*os.File, error) {
	if f, ok := fm.openFiles[filename]; ok {
		return f, nil
	}

	dbTable := path.Join(fm.dbDir, filename)
	f, err := os.OpenFile(dbTable, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	fm.openFiles[filename] = f
	return f, nil
}
