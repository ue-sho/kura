package com.uesho.kura.buffer

import com.uesho.kura.file.BlockId
import com.uesho.kura.file.FileManager
import com.uesho.kura.file.Page
import com.uesho.kura.log.LogManager
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File

/**
 * Integration test for buffer management with file operations.
 * Tests how buffers interact with the file system.
 */
class BufferFileTest {
    @TempDir
    lateinit var tempDir: File
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager
    private val blockSize = 400
    private val numberOfBuffers = 8
    private val testFile = "testfile"
    private val blockNumber = 2

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
    fun `should store string and integer values in buffer and persist to disk`() {
        val blockId = BlockId(testFile, blockNumber)

        // Get a buffer for our block
        val buffer1 = bufferManager.pin(blockId)
        val page1 = buffer1.getContents()

        // Write a string and an integer to the buffer
        val position1 = 88
        val testString = "abcdefghijklm"
        page1.setString(position1, testString)

        // Calculate the position for the integer, which comes after the string
        val stringSize = Page.maxLength(testString.length)
        val position2 = position1 + stringSize
        val testInt = 345
        page1.setInt(position2, testInt)

        // Mark the buffer as modified and unpin it
        buffer1.setModified(1, 0)
        bufferManager.unpin(buffer1)

        // Now read the data back from a different buffer
        val buffer2 = bufferManager.pin(blockId)
        val page2 = buffer2.getContents()

        // Verify the integer and string were stored correctly
        assertEquals(testInt, page2.getInt(position2))
        assertEquals(testString, page2.getString(position1))

        // Unpin the second buffer
        bufferManager.unpin(buffer2)
    }

    @Test
    fun `should reuse same buffer when pinning same block multiple times`() {
        val blockId = BlockId(testFile, blockNumber)

        // Pin a buffer to our block and write some data
        val buffer1 = bufferManager.pin(blockId)
        val page1 = buffer1.getContents()
        val position = 100
        val testInt = 500
        page1.setInt(position, testInt)
        buffer1.setModified(1, 0)

        // Keep the buffer pinned and get another buffer for the same block
        val buffer2 = bufferManager.pin(blockId)

        // These should be the same buffer object
        assertEquals(buffer1, buffer2)

        // Verify the data is visible in the second reference
        assertEquals(testInt, buffer2.getContents().getInt(position))

        // Unpin both references to the buffer
        bufferManager.unpin(buffer1)
        bufferManager.unpin(buffer2)
    }

    @Test
    fun `should replace unpinned buffers when all buffers are allocated`() {
        // Create more blocks than there are buffers
        val numBlocks = numberOfBuffers + 2
        val buffers = Array<Buffer?>(numBlocks) { null }

        // Pin buffers to different blocks
        for (i in 0 until numberOfBuffers) {
            val blockId = BlockId(testFile, i)
            buffers[i] = bufferManager.pin(blockId)

            // Write identifying data to each buffer
            buffers[i]?.getContents()?.setInt(0, i)
            buffers[i]?.setModified(1, 0)
        }

        // Unpin the first two buffers
        bufferManager.unpin(buffers[0]!!)
        bufferManager.unpin(buffers[1]!!)

        // Pin two more blocks, which should reuse the unpinned buffers
        for (i in numberOfBuffers until numBlocks) {
            val blockId = BlockId(testFile, i)
            buffers[i] = bufferManager.pin(blockId)

            // Write identifying data to these buffers too
            buffers[i]?.getContents()?.setInt(0, i)
            buffers[i]?.setModified(1, 0)
        }

        // Unpin all buffers
        for (i in 2 until numBlocks) {
            if (buffers[i] != null) {
                bufferManager.unpin(buffers[i]!!)
            }
        }

        // Verify data persistence by reading back blocks
        for (i in 0 until numBlocks) {
            val blockId = BlockId(testFile, i)
            val buffer = bufferManager.pin(blockId)
            assertEquals(i, buffer.getContents().getInt(0))
            bufferManager.unpin(buffer)
        }
    }
}