package com.uesho.kura.buffer

import com.uesho.kura.file.BlockId
import com.uesho.kura.file.FileManager
import com.uesho.kura.log.LogManager
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.io.TempDir
import java.io.File

/**
 * Test class for the BufferManager class.
 */
class BufferManagerTest {
    @TempDir
    lateinit var tempDir: File
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager
    private val blockSize = 400
    private val numberOfBuffers = 3 // Small number of buffers for testing
    private val testFile = "testfile"

    @BeforeEach
    fun setup() {
        fileManager = FileManager(tempDir, blockSize)
        logManager = LogManager(fileManager, "logfile")
        bufferManager = BufferManager(fileManager, logManager, numberOfBuffers)
    }

    @AfterEach
    fun cleanup() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `should track available buffers when pinning and unpinning`() {
        // All buffers should be initially available
        assertEquals(numberOfBuffers, bufferManager.available())

        // Pin a buffer
        val buffer = bufferManager.pin(BlockId(testFile, 0))

        // Now there should be one less available buffer
        assertEquals(numberOfBuffers - 1, bufferManager.available())

        // Unpin the buffer
        bufferManager.unpin(buffer)

        // All buffers should be available again
        assertEquals(numberOfBuffers, bufferManager.available())
    }

    @Test
    fun `should handle multiple pins and throw exception when no buffers are available`() {
        // Create an array of buffers
        val buffers = Array<Buffer?>(numberOfBuffers + 3) { null }

        // Pin 3 different blocks
        buffers[0] = bufferManager.pin(BlockId(testFile, 0))
        buffers[1] = bufferManager.pin(BlockId(testFile, 1))
        buffers[2] = bufferManager.pin(BlockId(testFile, 2))

        // Unpin the buffer for block 1
        bufferManager.unpin(buffers[1]!!)
        buffers[1] = null

        // Pin the same block 0 again (should reuse the existing buffer)
        buffers[3] = bufferManager.pin(BlockId(testFile, 0))

        // Pin block 1 again (should allocate a new buffer)
        buffers[4] = bufferManager.pin(BlockId(testFile, 1))

        // All buffers are now pinned
        assertEquals(0, bufferManager.available())

        // Trying to pin another block should throw an exception
        assertThrows(BufferAbortException::class.java) {
            bufferManager.pin(BlockId(testFile, 3))
        }

        // Unpin a buffer to make room
        bufferManager.unpin(buffers[2]!!)
        buffers[2] = null

        // Now we should be able to pin another block
        buffers[5] = bufferManager.pin(BlockId(testFile, 3))
        assertNotNull(buffers[5])

        // Verify the blocks that the buffers are pinned to
        assertEquals(BlockId(testFile, 0), buffers[0]?.getBlock())
        assertEquals(BlockId(testFile, 0), buffers[3]?.getBlock())
        assertEquals(BlockId(testFile, 1), buffers[4]?.getBlock())
        assertEquals(BlockId(testFile, 3), buffers[5]?.getBlock())
    }

    @Test
    fun `should flush only buffers modified by specified transaction`() {
        // Pin three blocks
        val buffer0 = bufferManager.pin(BlockId(testFile, 0))
        val buffer1 = bufferManager.pin(BlockId(testFile, 1))
        val buffer2 = bufferManager.pin(BlockId(testFile, 2))

        // Modify the buffers
        val txNum = 1
        val position = 80

        buffer0.getContents().setInt(position, 100)
        buffer0.setModified(txNum, 0)

        buffer1.getContents().setInt(position, 200)
        buffer1.setModified(txNum, 1)

        buffer2.getContents().setInt(position, 300)
        buffer2.setModified(2, 2) // Different transaction number

        // Flush all buffers for transaction 1
        bufferManager.flushAll(txNum)

        // Buffer 0 and 1 should no longer be dirty
        assertEquals(-1, buffer0.getModifyingTransaction())
        assertEquals(-1, buffer1.getModifyingTransaction())

        // Buffer 2 should still be dirty
        assertEquals(2, buffer2.getModifyingTransaction())
    }

    @Test
    fun `should reuse buffer when pinning same block multiple times`() {
        // Pin the same block twice
        val buffer1 = bufferManager.pin(BlockId(testFile, 0))
        val buffer2 = bufferManager.pin(BlockId(testFile, 0))

        // We should get the same buffer object
        assertSame(buffer1, buffer2)

        // Unpin once - should still be pinned
        bufferManager.unpin(buffer1)
        assertTrue(buffer1.isPinned())

        // Unpin again - should no longer be pinned
        bufferManager.unpin(buffer2)
        assertFalse(buffer1.isPinned())
    }
}