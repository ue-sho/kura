package com.kura.log

import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.file.Page
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class LogManagerTest {
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private val logFile = "test.log"
    private val blockSize = 400

    @BeforeEach
    fun setUp(@TempDir tempDir: Path) {
        fileManager = mockk(relaxed = true)
        every { fileManager.blockSize() } returns blockSize
        logManager = LogManager(fileManager, logFile)
    }

    @Test
    fun `should create new log file when it does not exist`() {
        // Arrange
        every { fileManager.length(logFile) } returns 0

        // Act
        LogManager(fileManager, logFile)

        // Assert
        verify {
            fileManager.append(logFile)
            fileManager.write(any(), any())
        }
    }

    @Test
    fun `should append log record and return incremented LSN`() {
        // Arrange
        val logRecord = "test log".toByteArray()
        val pageSlot = slot<Page>()
        every { fileManager.write(any(), capture(pageSlot)) } answers { }

        // Act
        val lsn1 = logManager.append(logRecord)
        val lsn2 = logManager.append(logRecord)

        // Assert
        assertEquals(1, lsn1)
        assertEquals(2, lsn2)
    }

    @Test
    fun `should create new block when current block is full`() {
        // Arrange
        val largeRecord = ByteArray(blockSize - Int.SIZE_BYTES)
        val blockIdSlot = slot<BlockId>()
        every { fileManager.append(logFile) } returns BlockId(logFile, 1)
        every { fileManager.write(capture(blockIdSlot), any()) } answers { }

        // Act
        logManager.append(largeRecord)

        // Assert
        verify {
            fileManager.append(logFile)
            fileManager.write(any(), any())
        }
    }

    @Test
    fun `should flush log records when requested`() {
        // Arrange
        val logRecord = "test log".toByteArray()
        val lsn = logManager.append(logRecord)

        // Act
        logManager.flush(lsn)

        // Assert
        verify(exactly = 2) {
            fileManager.write(any(), any())
        }
    }
}
