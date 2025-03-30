package com.kura.transaction.recovery

import com.kura.file.BlockId
import com.kura.file.Page
import com.kura.log.LogManager
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SetIntRecordTest {

    @Test
    fun `writeToLog should create correct log record`() {
        // Arrange
        val logManager = mockk<LogManager>()
        val txNum = 1
        val blockId = BlockId("testfile", 2)
        val offset = 100
        val value = 42
        val lsn = 5

        val byteArraySlot = slot<ByteArray>()

        every { logManager.append(capture(byteArraySlot)) } returns lsn

        // Act
        val result = SetIntRecord.writeToLog(logManager, txNum, blockId, offset, value)

        // Assert
        assertEquals(lsn, result)

        // Verify the log record contents
        val page = Page(byteArraySlot.captured)
        assertEquals(LogRecord.SET_INT, page.getInt(0)) // Operation type
        assertEquals(txNum, page.getInt(4)) // Transaction number
        assertEquals(blockId.fileName, page.getString(8)) // Filename

        // Also verify block number, offset, and value
        verify { logManager.append(any()) }
    }

    @Test
    fun `constructor should initialize fields correctly`() {
        // Arrange
        val txNum = 1
        val offset = 100
        val value = 42
        val fileName = "testfile"
        val blockNum = 2
        val blockId = BlockId(fileName, blockNum)

        // Create byte array for log record with correct size calculation
        val fnameSize = Page.maxLength(fileName.length)

        // Calculate size based on SetIntRecord implementation
        // Operation type + tx number + filename + block number + offset + value
        val logRecordSize = 4 + 4 + fnameSize + 4 + 4 + 4
        val bytes = ByteArray(logRecordSize)
        val page = Page(bytes)

        // Set data in the page
        page.setInt(0, LogRecord.SET_INT) // Operation type
        page.setInt(4, txNum) // Transaction number
        page.setString(8, blockId.fileName) // Filename

        // Calculate positions to match SetIntRecord implementation
        val bPos = 8 + fnameSize
        val oPos = bPos + 4
        val vPos = oPos + 4

        page.setInt(bPos, blockId.blockNum) // Block number
        page.setInt(oPos, offset) // Offset
        page.setInt(vPos, value) // Value

        // Act
        val record = SetIntRecord(page)

        // Assert
        assertEquals(LogRecord.SET_INT, record.operation())
        assertEquals(txNum, record.transactionNumber())

        // Also verify the string representation
        val recordString = record.toString()
        assert(recordString.contains("SET_INT"))
        assert(recordString.contains(txNum.toString()))
        assert(recordString.contains(blockId.toString()))
        assert(recordString.contains(offset.toString()))
        assert(recordString.contains(value.toString()))
    }

    @Test
    fun `undo should restore original value`() {
        // Arrange
        val txNum = 1
        val offset = 100
        val value = 42
        val fileName = "testfile"
        val blockNum = 2
        val blockId = BlockId(fileName, blockNum)

        // Create byte array for log record with correct size calculation
        val fnameSize = Page.maxLength(fileName.length)

        // Calculate size based on SetIntRecord implementation
        // Operation type + tx number + filename + block number + offset + value
        val logRecordSize = 4 + 4 + fnameSize + 4 + 4 + 4
        val bytes = ByteArray(logRecordSize)
        val page = Page(bytes)

        // Set data in the page
        page.setInt(0, LogRecord.SET_INT) // Operation type
        page.setInt(4, txNum) // Transaction number
        page.setString(8, blockId.fileName) // Filename

        // Calculate positions to match SetIntRecord implementation
        val bPos = 8 + fnameSize
        val oPos = bPos + 4
        val vPos = oPos + 4

        page.setInt(bPos, blockId.blockNum) // Block number
        page.setInt(oPos, offset) // Offset
        page.setInt(vPos, value) // Value

        val record = SetIntRecord(page)

        val transaction = mockk<Transaction>(relaxed = true)

        // Act
        record.undo(transaction)

        // Assert
        verify {
            transaction.pin(blockId)
            transaction.setInt(blockId, offset, value, false) // Restore original value, don't log
            transaction.unpin(blockId)
        }
    }
}