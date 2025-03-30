package com.kura.transaction.recovery

import com.kura.buffer.Buffer
import com.kura.buffer.BufferManager
import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.file.Page
import com.kura.log.LogManager
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RecoveryManagerTest {

    private lateinit var transaction: Transaction
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager
    private lateinit var recoveryManager: RecoveryManager
    private val transactionNumber = 1

    @BeforeEach
    fun setup() {
        transaction = mockk(relaxed = true)
        logManager = mockk(relaxed = true)
        bufferManager = mockk(relaxed = true)

        // Mock StartRecord.writeToLog
        mockkObject(StartRecord.Companion)
        every { StartRecord.writeToLog(any(), any()) } returns 1

        recoveryManager = RecoveryManager(transaction, transactionNumber, logManager, bufferManager)
    }

    @Test
    fun `commit should flush buffers and write commit record`() {
        // Arrange
        mockkObject(CommitRecord.Companion)
        every { CommitRecord.writeToLog(any(), any()) } returns 2

        // Act
        recoveryManager.commit()

        // Assert
        verify {
            bufferManager.flushAll(transactionNumber)
            CommitRecord.writeToLog(logManager, transactionNumber)
            logManager.flush(2)
        }
    }

    @Test
    fun `rollback should undo changes, flush buffers and write rollback record`() {
        // Arrange
        mockkObject(RollbackRecord.Companion)
        every { RollbackRecord.writeToLog(any(), any()) } returns 3

        // Act
        recoveryManager.rollback()

        // Assert
        verify {
            bufferManager.flushAll(transactionNumber)
            RollbackRecord.writeToLog(logManager, transactionNumber)
            logManager.flush(3)
        }
    }

    @Test
    fun `recover should undo uncommitted transactions and write checkpoint record`() {
        // Arrange
        mockkObject(CheckpointRecord.Companion)
        every { CheckpointRecord.writeToLog(any()) } returns 4

        // Act
        recoveryManager.recover()

        // Assert
        verify {
            bufferManager.flushAll(transactionNumber)
            CheckpointRecord.writeToLog(logManager)
            logManager.flush(4)
        }
    }

    @Test
    fun `setInt should write log record and return lsn`() {
        // Arrange
        val buffer = mockk<Buffer>()
        val page = mockk<Page>()
        val blockId = BlockId("testfile", 1)
        val offset = 100
        val oldValue = 42
        val lsn = 5

        every { buffer.contents() } returns page
        every { page.getInt(offset) } returns oldValue
        every { buffer.block() } returns blockId

        mockkObject(SetIntRecord.Companion)
        every { SetIntRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue) } returns lsn

        // Act
        val result = recoveryManager.setInt(buffer, offset, 99)

        // Assert
        verify {
            buffer.contents()
            page.getInt(offset)
            buffer.block()
            SetIntRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue)
        }

        assert(result == lsn) { "Expected LSN $lsn but got $result" }
    }

    @Test
    fun `setString should write log record and return lsn`() {
        // Arrange
        val buffer = mockk<Buffer>()
        val page = mockk<Page>()
        val blockId = BlockId("testfile", 1)
        val offset = 200
        val oldValue = "old"
        val lsn = 6

        every { buffer.contents() } returns page
        every { page.getString(offset) } returns oldValue
        every { buffer.block() } returns blockId

        mockkObject(SetStringRecord.Companion)
        every { SetStringRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue) } returns lsn

        // Act
        val result = recoveryManager.setString(buffer, offset, "new")

        // Assert
        verify {
            buffer.contents()
            page.getString(offset)
            buffer.block()
            SetStringRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue)
        }

        assert(result == lsn) { "Expected LSN $lsn but got $result" }
    }
}