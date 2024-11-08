package log_test

import (
	"fmt"
	"kura/internal/file"
	"kura/internal/log"
	"kura/internal/server"
	"path"
	"testing"
)

func TestLog(t *testing.T) {
	// Arrange
	db, err := server.NewKuraDB(path.Join(t.TempDir(), "logtest"), 400, 8)
	if err != nil {
		t.Fatalf("NewKuraDB: %v", err)
	}
	logManager := db.LogManager()

	// Act
	createRecords(t, logManager, 1, 35)
	printLogRecords(logManager, "The log file now has these records:")

	createRecords(t, logManager, 36, 70)
	logManager.Flush(65)
	printLogRecords(logManager, "The log file now has these records:")

	// Assert
	expectedRecords := map[int]string{}
	for i := 1; i <= 70; i++ {
		expectedRecords[i+100] = fmt.Sprintf("record%d", i)
	}

	iter := logManager.Iterator()
	for iter.HasNext() {
		record := iter.Next()
		page := file.NewPageFromBytes(record)
		str := page.GetString(0)
		nextPos := file.MaxLength(len(str))
		val := page.GetInt(nextPos)

		expectedStr, exists := expectedRecords[int(val)]
		if !exists {
			t.Errorf("Unexpected record found: [%s, %d]", str, val)
		} else if str != expectedStr {
			t.Errorf("Expected record: [%s, %d], but got: [%s, %d]", expectedStr, val, str, val)
		}
		delete(expectedRecords, int(val))
	}

	if len(expectedRecords) != 0 {
		t.Errorf("Missing records: %v", expectedRecords)
	}
}

func TestLogManager_NextBlock(t *testing.T) {
	// Arrange
	fileManager, err := file.NewFileManager(t.TempDir(), 100) // Smaller block size for testing
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}

	logFile := "testlogfile"
	logManager, err := log.NewLogManager(fileManager, logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}

	// Act: Append records until a new block is allocated
	expectedRecords := make([][]byte, 0)
	initialBlockID := logManager.CurrentBlockID().Number()

	// Append log records to fill up the first block
	for i := 0; ; i++ {
		record := createLogRecord(fmt.Sprintf("record%d", i), i+100)
		_, err := logManager.Append(record)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		expectedRecords = append(expectedRecords, record)

		// Check if a new block was allocated
		if logManager.CurrentBlockID().Number() != initialBlockID {
			break
		}
	}

	// Assert: Verify that a new block was allocated
	newBlockID := logManager.CurrentBlockID().Number()
	if newBlockID == initialBlockID {
		t.Fatalf("Expected a new block to be allocated, but block ID is still %d", initialBlockID)
	}

	// Act & Assert: Use Iterator to verify records in reverse order
	iter := logManager.Iterator()
	var actualRecords [][]byte
	for iter.HasNext() {
		record := iter.Next()
		actualRecords = append(actualRecords, record)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Expected %d records, but got %d", len(expectedRecords), len(actualRecords))
	}
}

func printLogRecords(logManager *log.LogManager, msg string) {
	fmt.Println(msg)
	iter := logManager.Iterator()
	for iter.HasNext() {
		record := iter.Next()
		page := file.NewPageFromBytes(record)
		str := page.GetString(0)
		nextPos := file.MaxLength(len(str))
		val := page.GetInt(nextPos)
		fmt.Printf("[%s, %d]\n", str, val)
	}
	fmt.Println()
}

func createRecords(t *testing.T, logManager *log.LogManager, start, end int) {
	fmt.Println("Creating records: ")
	for i := start; i <= end; i++ {
		record := createLogRecord(fmt.Sprintf("record%d", i), i+100)
		lsn, err := logManager.Append(record)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		fmt.Println(lsn)
	}
	fmt.Println()
}

func createLogRecord(str string, num int) []byte {
	nextPos := file.MaxLength(len(str))
	buffer := make([]byte, nextPos+file.Int32Size)
	page := file.NewPageFromBytes(buffer)
	page.SetString(0, str)
	page.SetInt(nextPos, int32(num))
	return buffer
}
