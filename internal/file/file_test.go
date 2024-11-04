package file_test

import (
	"path"
	"testing"

	"kura/internal/file"
	"kura/internal/server"
)

func TestFile(t *testing.T) {
	t.Parallel()

	// Arrange
	db, err := server.NewKuraDB(path.Join(t.TempDir(), "filetest"), 400)
	if err != nil {
		t.Fatalf("NewKuraDB: %v", err)
	}
	fm := db.FileManager()

	// Act
	// Create a test page and write a string value at a specified position
	page1 := file.NewPage(fm.BlockSize())
	pos1 := 88
	strVal := "abcdefghijklm"
	page1.SetString(pos1, strVal)

	// Calculate the maximum length for the string and set an integer value in the next position
	size := file.MaxLength(len(strVal))
	pos2 := pos1 + size
	intVar := int32(345)
	page1.SetInt(pos2, intVar)

	// Create a block and write the page data to this block
	blk := file.NewBlockID("testfile", 2)
	err = fm.Write(blk, page1)
	if err != nil {
		t.Fatalf("Failed to write to block %v: %v", blk, err)
	}

	// Create a new page and read the data from the previously written block into this page
	p2 := file.NewPage(fm.BlockSize())
	err = fm.Read(blk, p2)
	if err != nil {
		t.Fatalf("Failed to read from block %v: %v", blk, err)
	}

	// Assert
	if p2.GetInt(pos2) != intVar {
		t.Errorf("expected %d, got %d", intVar, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != strVal {
		t.Errorf("expected %q, got %q", strVal, p2.GetString(pos1))
	}
}
