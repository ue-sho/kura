package com.uesho.kura.buffer

import com.uesho.kura.file.BlockId
import com.uesho.kura.file.FileManager
import com.uesho.kura.log.LogManager
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File

/**
 * Test class for the Buffer class.
 */
class BufferTest {
    @TempDir
    lateinit var tempDir: File
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var buffer: Buffer
    private val blockSize = 400
    private val testFile = "testfile"
    private val blockNumber = 1

    @BeforeEach
    fun setup() {
        fileManager = FileManager(tempDir, blockSize)
        logManager = LogManager(fileManager, "logfile")
        buffer = Buffer(fileManager, logManager)

        // Initialize block with data
        val block = BlockId(testFile, blockNumber)
        buffer.assignToBlock(block)
    }

    @AfterEach
    fun cleanup() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `should assign buffer to a block and retrieve the same block`() {
        val block = BlockId(testFile, blockNumber)
        buffer.assignToBlock(block)
        assertEquals(block, buffer.getBlock())
    }

    @Test
    fun `should modify buffer contents and store the value correctly`() {
        val position = 80
        val value = 100

        // Write a value to the buffer
        val page = buffer.getContents()
        page.setInt(position, value)
        buffer.setModified(1, 0) // Set modified with transaction id 1

        // Verify the value was written correctly
        assertEquals(value, page.getInt(position))
    }

    @Test
    fun `should track pin and unpin operations correctly`() {
        // Initially not pinned
        assertEquals(false, buffer.isPinned())

        // Pin the buffer
        buffer.pin()
        assertEquals(true, buffer.isPinned())

        // Unpin the buffer
        buffer.unpin()
        assertEquals(false, buffer.isPinned())
    }

    @Test
    fun `should store transaction information when buffer is modified`() {
        val txNum = 5
        val lsn = 10

        // Set the buffer as modified
        buffer.setModified(txNum, lsn)

        // Verify the modifying transaction is set correctly
        assertEquals(txNum, buffer.getModifyingTransaction())
    }

    @Test
    fun `should persist changes to disk when buffer is flushed`() {
        val position = 80
        val value = 200
        val txNum = 3
        val lsn = 15

        // Modify the buffer and mark it as modified
        val page = buffer.getContents()
        page.setInt(position, value)
        buffer.setModified(txNum, lsn)

        // Flush the buffer
        buffer.flush()

        // After flush, the transaction number should be reset
        assertEquals(-1, buffer.getModifyingTransaction())

        // Create a new buffer for the same block to verify persistence
        val newBuffer = Buffer(fileManager, logManager)
        newBuffer.assignToBlock(BlockId(testFile, blockNumber))

        // Verify the data was persisted
        assertEquals(value, newBuffer.getContents().getInt(position))
    }
}