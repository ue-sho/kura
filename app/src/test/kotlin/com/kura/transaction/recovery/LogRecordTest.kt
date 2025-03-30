package com.kura.transaction.recovery

import com.kura.file.Page
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LogRecordTest {

    @Test
    fun `createLogRecord should create CheckpointRecord for CHECKPOINT type`() {
        // Arrange
        val bytes = ByteArray(4)
        val page = Page(bytes)
        page.setInt(0, LogRecord.CHECKPOINT)

        // Act
        val record = LogRecord.createLogRecord(bytes)

        // Assert
        assert(record is CheckpointRecord)
        assertEquals(LogRecord.CHECKPOINT, record.operation())
    }

    @Test
    fun `createLogRecord should create StartRecord for START type`() {
        // Arrange
        val bytes = ByteArray(8)
        val page = Page(bytes)
        page.setInt(0, LogRecord.START)
        page.setInt(4, 1) // Transaction ID

        // Act
        val record = LogRecord.createLogRecord(bytes)

        // Assert
        assert(record is StartRecord)
        assertEquals(LogRecord.START, record.operation())
        assertEquals(1, record.transactionNumber())
    }

    @Test
    fun `createLogRecord should create CommitRecord for COMMIT type`() {
        // Arrange
        val bytes = ByteArray(8)
        val page = Page(bytes)
        page.setInt(0, LogRecord.COMMIT)
        page.setInt(4, 1) // Transaction ID

        // Act
        val record = LogRecord.createLogRecord(bytes)

        // Assert
        assert(record is CommitRecord)
        assertEquals(LogRecord.COMMIT, record.operation())
        assertEquals(1, record.transactionNumber())
    }

    @Test
    fun `createLogRecord should create RollbackRecord for ROLLBACK type`() {
        // Arrange
        val bytes = ByteArray(8)
        val page = Page(bytes)
        page.setInt(0, LogRecord.ROLLBACK)
        page.setInt(4, 1) // Transaction ID

        // Act
        val record = LogRecord.createLogRecord(bytes)

        // Assert
        assert(record is RollbackRecord)
        assertEquals(LogRecord.ROLLBACK, record.operation())
        assertEquals(1, record.transactionNumber())
    }

    @Test
    fun `createLogRecord should create SetIntRecord for SET_INT type`() {
        // Arrange
        val filename = "testfile"
        val fnameSize = Page.maxLength(filename.length)

        // Calculate required size: operation + txnum + filename + blocknum + offset + value
        val logRecordSize = 4 + 4 + fnameSize + 4 + 4 + 4
        val bytes = ByteArray(logRecordSize)
        val page = Page(bytes)

        page.setInt(0, LogRecord.SET_INT)
        page.setInt(4, 1) // Transaction ID
        page.setString(8, filename) // Filename

        // Positions calculated to match SetIntRecord implementation
        val bPos = 8 + fnameSize
        val oPos = bPos + 4
        val vPos = oPos + 4

        page.setInt(bPos, 2) // Block number
        page.setInt(oPos, 100) // Offset
        page.setInt(vPos, 42) // Value

        // Act
        val record = LogRecord.createLogRecord(bytes)

        // Assert
        assert(record is SetIntRecord)
        assertEquals(LogRecord.SET_INT, record.operation())
        assertEquals(1, record.transactionNumber())
    }

    @Test
    fun `createLogRecord should create SetStringRecord for SET_STRING type`() {
        // Arrange
        val filename = "testfile"
        val value = "test value"
        val fnameSize = Page.maxLength(filename.length)
        val valueSize = Page.maxLength(value.length)

        // Calculate required size: operation + txnum + filename + blocknum + offset + value
        val logRecordSize = 4 + 4 + fnameSize + 4 + 4 + valueSize
        val bytes = ByteArray(logRecordSize)
        val page = Page(bytes)

        page.setInt(0, LogRecord.SET_STRING)
        page.setInt(4, 1) // Transaction ID
        page.setString(8, filename) // Filename

        // Positions calculated to match SetStringRecord implementation
        val bPos = 8 + fnameSize
        val oPos = bPos + 4
        val vPos = oPos + 4

        page.setInt(bPos, 2) // Block number
        page.setInt(oPos, 100) // Offset
        page.setString(vPos, value) // Value

        // Act
        val record = LogRecord.createLogRecord(bytes)

        // Assert
        assert(record is SetStringRecord)
        assertEquals(LogRecord.SET_STRING, record.operation())
        assertEquals(1, record.transactionNumber())
    }

    @Test
    fun `createLogRecord should throw exception for unknown type`() {
        // Arrange
        val bytes = ByteArray(4)
        val page = Page(bytes)
        page.setInt(0, 99) // Unknown type

        // Act & Assert
        assertThrows<IllegalArgumentException> {
            LogRecord.createLogRecord(bytes)
        }
    }
}