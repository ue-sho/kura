package buffer_test

import (
	"fmt"
	"kura/internal/file"
	"kura/internal/server"
	"testing"
)

func TestBuffer(t *testing.T) {
	// Initialize SimpleDB with a buffer pool of 3 buffers
	db, err := server.NewKuraDB("buffertest", 400, 3)
	if err != nil {
		t.Fatalf("NewKuraDB: %v", err)
	}
	bm := db.BufferManager()

	// Pin the first buffer and modify its contents
	block1 := file.NewBlockID("testfile", 1)
	buff1, err := bm.Pin(block1)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block1, err)
		return
	}

	page := buff1.Contents()
	n := page.GetInt(80)
	page.SetInt(80, n+1)    // Modify the page content
	buff1.SetModified(1, 0) // Mark buffer as modified
	fmt.Printf("The new value is %d\n", n+1)
	bm.Unpin(buff1)

	// Pin multiple buffers, causing buff1 to be flushed to disk
	block2 := file.NewBlockID("testfile", 2)
	block3 := file.NewBlockID("testfile", 3)
	block4 := file.NewBlockID("testfile", 4)

	buff2, err := bm.Pin(block2)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block2, err)
		return
	}

	_, err = bm.Pin(block3)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block3, err)
		return
	}

	_, err = bm.Pin(block4)
	if err != nil {
		fmt.Printf("Failed to pin block %s: %v\n", block4, err)
		return
	}

	// Unpin buff2 and re-pin block1
	bm.Unpin(buff2)
	buff2, err = bm.Pin(block1)
	if err != nil {
		fmt.Printf("Failed to re-pin block %s: %v\n", block1, err)
		return
	}

	page2 := buff2.Contents()
	page2.SetInt(80, 9999)  // Modify the content again
	buff2.SetModified(1, 0) // Mark buffer as modified
	bm.Unpin(buff2)
}
