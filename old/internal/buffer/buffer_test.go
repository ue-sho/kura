package buffer_test

import (
	"fmt"
	"kura/internal/file"
	"kura/internal/server"
	"testing"
)

func TestBuffer(t *testing.T) {
	// Arrange
	db, err := server.NewKuraDB("buffertest", 400, 3)
	if err != nil {
		t.Fatalf("NewKuraDB: %v", err)
	}
	bm := db.BufferManager()

	// Act
	// Pin the first buffer and modify its contents
	block1 := file.NewBlockID("testfile", 1)
	buff1, err := bm.Pin(block1)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block1, err)
		return
	}
	page := buff1.Contents()
	n := page.GetInt(80)
	page.SetInt(80, n+1)
	buff1.SetModified(1, 0) // This modification will get written to disk.
	fmt.Printf("The new value is %d\n", n+1)
	bm.Unpin(buff1)

	// One of these pins will flush buff1 to disk:
	block2 := file.NewBlockID("testfile", 2)
	buff2, err := bm.Pin(block2)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block2, err)
		return
	}

	block3 := file.NewBlockID("testfile", 3)
	_, err = bm.Pin(block3)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block3, err)
		return
	}

	block4 := file.NewBlockID("testfile", 4)
	_, err = bm.Pin(block4)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block4, err)
		return
	}

	bm.Unpin(buff2)
	buff2, err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		fmt.Printf("Failed to re-pin block %s: %v\n", block1, err)
		return
	}

	page2 := buff2.Contents()
	page2.SetInt(80, 9999) //  This modification won't get written to disk.
	buff2.SetModified(1, 0)
	bm.Unpin(buff2)

	// Assert
	// page2 is a copy of the contents of block1, so it should have the same value as the last modification.
	buff3, err := bm.Pin(block1)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block1, err)
		return
	}
	page3 := buff3.Contents()
	if page3.GetInt(80) != n+1 {
		fmt.Printf("Expected %d, got %d\n", n+1, page3.GetInt(80))
	}
}
