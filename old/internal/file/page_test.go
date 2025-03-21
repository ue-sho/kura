package file_test

import (
	"bytes"
	"testing"
	"time"

	"kura/internal/file"
)

func TestPage(t *testing.T) {
	// Arrange
	blockSize := int64(1024)
	page := file.NewPage(blockSize)

	// Act/Assert
	// Test SetInt and GetInt
	offsetInt := 0
	expectedInt := int32(123456)
	page.SetInt(offsetInt, expectedInt)
	if result := page.GetInt(offsetInt); result != expectedInt {
		t.Errorf("GetInt() = %d; want %d", result, expectedInt)
	}

	// Test SetShort and GetShort
	offsetShort := 8
	expectedShort := int16(12345)
	page.SetShort(offsetShort, expectedShort)
	if result := page.GetShort(offsetShort); result != expectedShort {
		t.Errorf("GetShort() = %d; want %d", result, expectedShort)
	}

	// Test SetBool and GetBool
	offsetBool := 16
	expectedBool := true
	page.SetBool(offsetBool, expectedBool)
	if result := page.GetBool(offsetBool); result != expectedBool {
		t.Errorf("GetBool() = %v; want %v", result, expectedBool)
	}

	// Test SetDate and GetDate
	offsetDate := 20
	expectedDate := time.Now().Truncate(time.Second) // Truncate to remove extra precision
	page.SetDate(offsetDate, expectedDate)
	if result := page.GetDate(offsetDate); !result.Equal(expectedDate) {
		t.Errorf("GetDate() = %v; want %v", result, expectedDate)
	}

	// Test SetBytes and GetBytes
	offsetBytes := 40
	expectedBytes := []byte{10, 20, 30, 40, 50}
	page.SetBytes(offsetBytes, expectedBytes)
	if result := page.GetBytes(offsetBytes); !bytes.Equal(result, expectedBytes) {
		t.Errorf("GetBytes() = %v; want %v", result, expectedBytes)
	}

	// Test SetString and GetString
	offsetString := 60
	expectedString := "hello world"
	page.SetString(offsetString, expectedString)
	if result := page.GetString(offsetString); result != expectedString {
		t.Errorf("GetString() = %q; want %q", result, expectedString)
	}
}

func TestMaxLength(t *testing.T) {
	strLen := 10
	expectedMaxLength := strLen + 1 // +1 for the null terminator
	if result := file.MaxLength(strLen); result != expectedMaxLength {
		t.Errorf("MaxLength(%d) = %d; want %d", strLen, result, expectedMaxLength)
	}
}
