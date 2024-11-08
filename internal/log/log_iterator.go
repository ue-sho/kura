package log

import (
	"kura/internal/file"
)

type LogIterator struct {
	fileManager *file.FileManager
	blockID     *file.BlockID
	page        *file.Page
	currentPos  int64
	boundary    int64
}

func NewLogIterator(fileManager *file.FileManager, blockID *file.BlockID) *LogIterator {
	page := file.NewPage(fileManager.BlockSize())

	it := &LogIterator{
		fileManager: fileManager,
		blockID:     blockID,
		page:        page,
		currentPos:  0,
		boundary:    0,
	}
	it.moveToBlock(blockID)
	return it
}

func (it *LogIterator) moveToBlock(blockID *file.BlockID) error {
	it.fileManager.Read(blockID, it.page)
	it.boundary = int64(it.page.GetInt(0))
	it.currentPos = it.boundary
	return nil
}

func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fileManager.BlockSize() || it.blockID.Number() > 0
}

func (it *LogIterator) Next() []byte {
	if it.currentPos == it.fileManager.BlockSize() {
		it.blockID = file.NewBlockID(it.blockID.Filename(), it.blockID.Number()-1)
		it.moveToBlock(it.blockID)
	}

	record := it.page.GetBytes(int(it.currentPos))
	it.currentPos += int64(file.Int32Size + len(record))

	return record

}
