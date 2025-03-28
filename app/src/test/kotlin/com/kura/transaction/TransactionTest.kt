package com.kura.transaction

import com.kura.buffer.Buffer
import com.kura.buffer.BufferManager
import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.file.Page
import com.kura.log.LogManager
import com.kura.transaction.concurrency.ConcurrencyManager
import com.kura.transaction.recovery.RecoveryManager
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TransactionTest {
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager
    private lateinit var transaction: Transaction
    private lateinit var recoveryManager: RecoveryManager
    private lateinit var concurrencyManager: ConcurrencyManager
    private lateinit var bufferList: BufferList
    private val testFile = "testfile"
    private val blockSize = 400

    @BeforeEach
    fun setUp() {
        fileManager = mockk(relaxed = true)
        logManager = mockk(relaxed = true)
        bufferManager = mockk(relaxed = true)

        every { fileManager.blockSize() } returns blockSize

        // Mock internal components used inside Transaction
        recoveryManager = mockk(relaxed = true)
        concurrencyManager = mockk(relaxed = true)
        bufferList = mockk(relaxed = true)

        // Create a real Transaction but inject mocked dependencies
        transaction = Transaction(fileManager, logManager, bufferManager)

        // Use reflection to inject our mocked objects
        val recoveryManagerField = Transaction::class.java.getDeclaredField("recoveryManager")
        recoveryManagerField.isAccessible = true
        recoveryManagerField.set(transaction, recoveryManager)

        val concurrencyManagerField = Transaction::class.java.getDeclaredField("concurrencyManager")
        concurrencyManagerField.isAccessible = true
        concurrencyManagerField.set(transaction, concurrencyManager)

        val bufferListField = Transaction::class.java.getDeclaredField("bufferList")
        bufferListField.isAccessible = true
        bufferListField.set(transaction, bufferList)

        // Mock common concurrency manager operations
        every { concurrencyManager.sLock(any()) } just Runs
        every { concurrencyManager.xLock(any()) } just Runs
        every { concurrencyManager.release() } just Runs
    }

    @Test
    fun `should pin and unpin blocks`() {
        // Arrange
        val blockId = BlockId(testFile, 1)

        // Act
        transaction.pin(blockId)
        transaction.unpin(blockId)

        // Assert
        verify(exactly = 1) {
            bufferList.pin(blockId)
            bufferList.unpin(blockId)
        }
    }

    @Test
    fun `should read int value from block`() {
        // Arrange
        val blockId = BlockId(testFile, 1)
        val offset = 80
        val expectedValue = 42
        val buffer = mockk<Buffer>()
        val page = mockk<Page>()

        every { bufferList.getBuffer(blockId) } returns buffer
        every { buffer.contents() } returns page
        every { page.getInt(offset) } returns expectedValue

        // Act
        val actualValue = transaction.getInt(blockId, offset)

        // Assert
        assertEquals(expectedValue, actualValue)
        verify {
            concurrencyManager.sLock(blockId)
            bufferList.getBuffer(blockId)
            buffer.contents()
            page.getInt(offset)
        }
    }

    @Test
    fun `should read string value from block`() {
        // Arrange
        val blockId = BlockId(testFile, 1)
        val offset = 40
        val expectedValue = "test value"
        val buffer = mockk<Buffer>()
        val page = mockk<Page>()

        every { bufferList.getBuffer(blockId) } returns buffer
        every { buffer.contents() } returns page
        every { page.getString(offset) } returns expectedValue

        // Act
        val actualValue = transaction.getString(blockId, offset)

        // Assert
        assertEquals(expectedValue, actualValue)
        verify {
            concurrencyManager.sLock(blockId)
            bufferList.getBuffer(blockId)
            buffer.contents()
            page.getString(offset)
        }
    }

    @Test
    fun `should write int value to block`() {
        // Arrange
        val blockId = BlockId(testFile, 1)
        val offset = 80
        val value = 42
        val lsn = 1
        val buffer = mockk<Buffer>()
        val page = mockk<Page>()

        every { bufferList.getBuffer(blockId) } returns buffer
        every { buffer.contents() } returns page
        every { page.setInt(offset, value) } just Runs
        every { buffer.setModified(any(), lsn) } just Runs
        every { recoveryManager.setInt(buffer, offset, value) } returns lsn

        // Act
        transaction.setInt(blockId, offset, value, true)

        // Assert
        verify {
            concurrencyManager.xLock(blockId)
            bufferList.getBuffer(blockId)
            buffer.contents()
            page.setInt(offset, value)
            buffer.setModified(any(), lsn)
            recoveryManager.setInt(buffer, offset, value)
        }
    }

    @Test
    fun `should write string value to block`() {
        // Arrange
        val blockId = BlockId(testFile, 1)
        val offset = 40
        val value = "test value"
        val lsn = 1
        val buffer = mockk<Buffer>()
        val page = mockk<Page>()

        every { bufferList.getBuffer(blockId) } returns buffer
        every { buffer.contents() } returns page
        every { page.setString(offset, value) } just Runs
        every { buffer.setModified(any(), lsn) } just Runs
        every { recoveryManager.setString(buffer, offset, value) } returns lsn

        // Act
        transaction.setString(blockId, offset, value, true)

        // Assert
        verify {
            concurrencyManager.xLock(blockId)
            bufferList.getBuffer(blockId)
            buffer.contents()
            page.setString(offset, value)
            buffer.setModified(any(), lsn)
            recoveryManager.setString(buffer, offset, value)
        }
    }

    @Test
    fun `should commit transaction and release resources`() {
        // Arrange
        every { recoveryManager.commit() } just Runs
        every { bufferList.unpinAll() } just Runs

        // Act
        transaction.commit()

        // Assert
        verify {
            recoveryManager.commit()
            concurrencyManager.release()
            bufferList.unpinAll()
        }
    }

    @Test
    fun `should rollback transaction and release resources`() {
        // Arrange
        every { recoveryManager.rollback() } just Runs
        every { bufferList.unpinAll() } just Runs

        // Act
        transaction.rollback()

        // Assert
        verify {
            recoveryManager.rollback()
            concurrencyManager.release()
            bufferList.unpinAll()
        }
    }

    @Test
    fun `should get file size`() {
        // Arrange
        val filename = testFile
        val expectedSize = 5

        every { fileManager.length(filename) } returns expectedSize

        // Act
        val actualSize = transaction.size(filename)

        // Assert
        assertEquals(expectedSize, actualSize)
        verify {
            concurrencyManager.sLock(any())
            fileManager.length(filename)
        }
    }

    @Test
    fun `should append new block to file`() {
        // Arrange
        val filename = testFile
        val expectedBlockId = BlockId(filename, 1)

        every { fileManager.append(filename) } returns expectedBlockId

        // Act
        val actualBlockId = transaction.append(filename)

        // Assert
        assertEquals(expectedBlockId, actualBlockId)
        verify {
            concurrencyManager.xLock(any())
            fileManager.append(filename)
        }
    }
}