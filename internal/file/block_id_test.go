package file_test

import (
	"testing"

	"kura/internal/file"
)

func TestBlockID(t *testing.T) {
	// Arrange
	filename := "testfile"
	blockNum := int64(123)
	blockID := file.NewBlockID(filename, blockNum)

	// Act/Assert
	// Test Filename method
	if blockID.Filename() != filename {
		t.Errorf("Filename() = %s; want %s", blockID.Filename(), filename)
	}

	// Test Number method
	if blockID.Number() != blockNum {
		t.Errorf("Number() = %d; want %d", blockID.Number(), blockNum)
	}

	// Test String method
	expectedString := "[file testfile, block 123]"
	if blockID.String() != expectedString {
		t.Errorf("String() = %s; want %s", blockID.String(), expectedString)
	}

	// Test Hash method
	hash1 := blockID.Hash()
	hash2 := blockID.Hash()
	if hash1 != hash2 {
		t.Errorf("Hash() is not consistent; got %d and %d", hash1, hash2)
	}
}
