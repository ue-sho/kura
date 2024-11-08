package buffer_test

import (
	"fmt"
	"kura/internal/buffer"
	"kura/internal/file"
	"kura/internal/server"
	"testing"
)

func TestBufferManager(t *testing.T) {
	// Arrange
	db, err := server.NewKuraDB("buffermgrtest", 400, 3)
	if err != nil {
		t.Fatalf("NewKuraDB: %v", err)
	}
	bm := db.BufferManager()

	// Act
	// Pin blocks to buffers
	buffers := make([]*buffer.Buffer, 6)
	buffers[0], err = bm.Pin(file.NewBlockID("testfile", 0))
	if err != nil {
		fmt.Printf("Failed to pin 0: %v\n", err)
	}
	buffers[1], err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		fmt.Printf("Failed to pin 1: %v\n", err)
	}
	buffers[2], err = bm.Pin(file.NewBlockID("testfile", 2))
	if err != nil {
		fmt.Printf("Failed to pin 2: %v\n", err)
	}

	// Unpin buffer 1
	bm.Unpin(buffers[1])
	buffers[1] = nil

	// Pin block 0 again
	buffers[3], err = bm.Pin(file.NewBlockID("testfile", 0))
	if err != nil {
		fmt.Printf("Failed to pin 3: %v\n", err)
	}

	// Pin block 1 again
	buffers[4], err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		fmt.Printf("Failed to repin 1: %v\n", err)
	}
	fmt.Printf("Available buffers: %d\n", bm.Available())

	// Attempt to pin block 3, expecting an exception
	fmt.Println("Attempting to pin block 3...")
	if buffer, err := bm.Pin(file.NewBlockID("testfile", 3)); err != nil {
		fmt.Println("Exception: No available buffers")
	} else {
		buffers[5] = buffer
	}

	// Unpin buffer 2 and try pinning block 3 again, now it should work
	bm.Unpin(buffers[2])
	buffers[2] = nil

	buffers[5], err = bm.Pin(file.NewBlockID("testfile", 3))
	if err != nil {
		fmt.Printf("Failed to pin 5: %v\n", err)
	}

	// Display buffer allocations
	fmt.Println("Final Buffer Allocation:")
	for i, buffer := range buffers {
		if buffer != nil {
			fmt.Printf("Buffer %d is allocated to block %s\n", i, buffer.Block().String())
		} else {
			fmt.Printf("Buffer %d is not allocated\n", i)
		}
	}
}
