package buffer_test

import (
	"fmt"
	"kura/internal/buffer"
	"kura/internal/file"
	"kura/internal/server"
	"testing"
)

func TestBufferManager(t *testing.T) {
	// Initialize SimpleDB with a buffer pool of 3 buffers
	db, err := server.NewKuraDB("buffermgrtest", 400, 3)
	if err != nil {
		t.Fatalf("NewKuraDB: %v", err)
	}
	bm := db.BufferManager()

	// Pin blocks to buffers
	buffers := make([]*buffer.Buffer, 6)
	buffers[0], _ = bm.Pin(file.NewBlockID("testfile", 0))
	buffers[1], _ = bm.Pin(file.NewBlockID("testfile", 1))
	buffers[2], _ = bm.Pin(file.NewBlockID("testfile", 2))

	// Unpin buffer 1
	bm.Unpin(buffers[1])
	buffers[1] = nil

	// Pin block 0 again
	buffers[3], _ = bm.Pin(file.NewBlockID("testfile", 0))

	// Pin block 1 again
	buffers[4], _ = bm.Pin(file.NewBlockID("testfile", 1))
	fmt.Printf("Available buffers: %d\n", bm.Available())

	// Attempt to pin block 3, expecting an exception
	fmt.Println("Attempting to pin block 3...")
	if buffer, err := bm.Pin(file.NewBlockID("testfile", 3)); err != nil {
		fmt.Println("Exception: No available buffers\n")
	} else {
		buffers[5] = buffer
	}

	// Unpin buffer 2 and try pinning block 3 again, now it should work
	bm.Unpin(buffers[2])
	buffers[2] = nil

	buffers[5], _ = bm.Pin(file.NewBlockID("testfile", 3))
	fmt.Println("Final Buffer Allocation:")

	// Display buffer allocations
	for i, buffer := range buffers {
		if buffer != nil {
			fmt.Printf("Buffer %d is allocated to block %s\n", i, buffer.Block().String())
		} else {
			fmt.Printf("Buffer %d is not allocated\n", i)
		}
	}
}
