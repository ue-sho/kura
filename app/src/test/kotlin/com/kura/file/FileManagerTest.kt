package com.kura.file

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class FileManagerTest {
    @TempDir
    lateinit var tempDir: File

    private lateinit var fileManager: FileManager
    private val blockSize = 400

    @BeforeEach
    fun setUp() {
        fileManager = FileManager(tempDir, blockSize)
    }

    @Test
    fun `test file manager initialization`() {
        assertFalse(fileManager.isNew(), "New database directory should be marked as new")
    }

    @Test
    fun `test read and write operations`() {
        // Arrange
        val filename = "test.db"
        val block = BlockId(filename, 0)
        val page = Page(blockSize)
        val testData = 12345

        // Act
        page.setInt(0, testData)
        fileManager.write(block, page)

        val readPage = Page(blockSize)
        fileManager.read(block, readPage)

        // Assert
        assertEquals(testData, readPage.getInt(0), "Read data should match written data")
    }

    @Test
    fun `test append operation`() {
        // Arrange
        val filename = "test.db"

        // Act
        val block1 = fileManager.append(filename)
        val block2 = fileManager.append(filename)

        // Assert
        assertEquals(0, block1.blockNum, "First block should have number 0")
        assertEquals(1, block2.blockNum, "Second block should have number 1")
        assertEquals(2, fileManager.length(filename), "File should have 2 blocks")
    }

    @Test
    fun `test length operation`() {
        // Arrange
        val filename = "test.db"

        // Act & Assert
        assertEquals(0, fileManager.length(filename), "New file should have length 0")

        // Append some blocks
        fileManager.append(filename)
        fileManager.append(filename)
        fileManager.append(filename)

        assertEquals(3, fileManager.length(filename), "File should have 3 blocks after appending")
    }

    @Test
    fun `test temporary file cleanup`() {
        // Arrange
        val tempFile = File(tempDir, "temp_test.db")
        tempFile.createNewFile()

        // Act
        FileManager(tempDir, blockSize) // Create new file manager, which should clean up temp files

        // Assert
        assertFalse(tempFile.exists(), "Temporary file should be deleted during initialization")
    }

    @Test
    fun `test concurrent access`() {
        // Arrange
        val filename = "test.db"
        val block = BlockId(filename, 0)
        val page = Page(blockSize)
        val testData = 12345

        // Act & Assert
        val threads = List(10) { threadNum ->
            Thread {
                page.setInt(0, testData + threadNum)
                fileManager.write(block, page)

                val readPage = Page(blockSize)
                fileManager.read(block, readPage)
                assertTrue(readPage.getInt(0) >= testData, "Read data should be valid after concurrent access")
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }
    }
}