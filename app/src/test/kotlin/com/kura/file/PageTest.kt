package com.kura.file

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class PageTest {
    @Test
    fun `test page operations`() {
        // Arrange
        val blockSize = 1024
        val page = Page(blockSize)

        // Test Int operations
        val offsetInt = 0
        val expectedInt = 123456
        page.setInt(offsetInt, expectedInt)
        assertEquals(expectedInt, page.getInt(offsetInt), "Int value should match")

        // // Test Short operations
        // val offsetShort = 8
        // val expectedShort: Short = 12345
        // page.setShort(offsetShort, expectedShort)
        // assertEquals(expectedShort, page.getShort(offsetShort), "Short value should match")

        // // Test Boolean operations
        // val offsetBool = 16
        // val expectedBool = true
        // page.setBool(offsetBool, expectedBool)
        // assertEquals(expectedBool, page.getBool(offsetBool), "Boolean value should match")

        // // Test Date operations
        // val offsetDate = 20
        // val expectedDate = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        // page.setDate(offsetDate, expectedDate)
        // assertEquals(expectedDate, page.getDate(offsetDate), "Date value should match")

        // Test Bytes operations
        val offsetBytes = 40
        val expectedBytes = byteArrayOf(10, 20, 30, 40, 50)
        page.setBytes(offsetBytes, expectedBytes)
        assertArrayEquals(expectedBytes, page.getBytes(offsetBytes), "Byte array should match")

        // Test String operations
        val offsetString = 60
        val expectedString = "hello world"
        page.setString(offsetString, expectedString)
        assertEquals(expectedString, page.getString(offsetString), "String value should match")
    }

    @Test
    fun `test maxLength calculation`() {
        val strLen = 10
        val expectedMaxLength = strLen + 4 // +4 is int size
        assertEquals(expectedMaxLength, Page.maxLength(strLen), "MaxLength should include null terminator")
    }

    @Test
    fun `test page creation from byte array`() {
        // Arrange
        val bytes = ByteArray(100)
        val value = 12345
        val offset = 0

        // Act
        val page = Page(bytes)
        page.setInt(offset, value)

        // Assert
        assertEquals(value, page.getInt(offset), "Value should be readable after writing to byte array-based page")
    }
}