package com.uesho.kura.log

import com.uesho.kura.file.BlockId
import com.uesho.kura.file.FileManager
import com.uesho.kura.file.Page
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class LogIteratorTest {
    private lateinit var fileManager: FileManager
    private val blockSize = 400
    private val logFile = "test.log"

    @BeforeEach
    fun setUp() {
        fileManager = mockk(relaxed = true)
        every { fileManager.blockSize() } returns blockSize
    }

    @Test
    fun `should have next when current position is less than block size`() {
        // Given: Setup the block and mock file manager to simulate a non-empty block
        val block = BlockId(logFile, 0)
        val boundaryPosition = blockSize - 10 // Some position before the end of block

        // Mock the read method called during LogIterator initialization
        // This sets the currentPos to a value less than blockSize
        every { fileManager.read(any(), any()) } answers {
            val page = arg<Page>(1)
            page.setInt(0, boundaryPosition)
        }

        // Create iterator with the mock setup
        val logIterator = LogIterator(fileManager, block)

        // When: Check if there are more records
        val hasNext = logIterator.hasNext()

        // Then: Should have next because currentPos < blockSize
        assertTrue(hasNext)
    }

    @Test
    fun `should have next when there are previous blocks`() {
        // Given: Setup a block with block number > 0 (indicating previous blocks exist)
        val block = BlockId(logFile, 1)

        // Mock read to set current position at the end of this block
        every { fileManager.read(any(), any()) } answers {
            val page = arg<Page>(1)
            page.setInt(0, blockSize) // Position at end of block
        }

        // Create iterator with this setup
        val logIterator = LogIterator(fileManager, block)

        // When: Check if there are more records
        val hasNext = logIterator.hasNext()

        // Then: Should have next because block.blockNum > 0, even though currentPos = blockSize
        assertTrue(hasNext)
    }

    @Test
    fun `should not have next when at end of last block`() {
        // Given: Setup block 0 (the first/last block) and position at its end
        val block = BlockId(logFile, 0)

        // Mock read to set current position at the end of the block
        every { fileManager.read(any(), any()) } answers {
            val page = arg<Page>(1)
            page.setInt(0, blockSize) // Position at end of block
        }

        // Create iterator in this terminal state
        val logIterator = LogIterator(fileManager, block)

        // When: Check if there are more records
        val hasNext = logIterator.hasNext()

        // Then: No more records because currentPos = blockSize and block.blockNum = 0
        assertFalse(hasNext)
    }

    @Test
    fun `should move to previous block when current block is exhausted`() {
        // Given: Setup iterator with current position at the end of a non-first block
        val block = BlockId(logFile, 1)

        // Set the initial read to position at end of block
        every { fileManager.read(eq(block), any()) } answers {
            val page = arg<Page>(1)
            page.setInt(0, blockSize) // Position at end of current block
        }

        // Set up read for the previous block (will be called by next())
        every { fileManager.read(eq(BlockId(logFile, 0)), any()) } answers {
            val page = arg<Page>(1)
            page.setInt(0, blockSize - 20) // Some valid position in previous block
        }

        val logIterator = LogIterator(fileManager, block)

        // When: Call next() which should move to previous block since we're at the end
        logIterator.next()

        // Then: Should have read the previous block
        verify {
            fileManager.read(BlockId(logFile, 0), any())
        }
    }

    @Test
    fun `should read log record from current position`() {
        // Given: Setup block and test record
        val block = BlockId(logFile, 0)
        val testRecord = "test log".toByteArray()
        val recordPosition = blockSize - testRecord.size - Int.SIZE_BYTES

        // Mock read to set up the page with our test record
        every { fileManager.read(any(), any()) } answers {
            val page = arg<Page>(1)
            page.setInt(0, recordPosition) // Set boundary to point to our record
            page.setBytes(recordPosition, testRecord) // Place the record at this position
        }

        val logIterator = LogIterator(fileManager, block)

        // When: Get the next record
        val record = logIterator.next()

        // Then: Should get our test record
        assertArrayEquals(testRecord, record)
    }
}