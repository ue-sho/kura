package com.kura.transaction

import com.kura.buffer.BufferManager
import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.log.LogManager
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class TransactionIntegrationTest {
    @TempDir
    lateinit var tempDir: Path

    @Nested
    inner class BasicTransactionOperationsTest {
        private lateinit var fileManager: FileManager
        private lateinit var logManager: LogManager
        private lateinit var bufferManager: BufferManager
        private val testFile = "testfile"
        private val blockSize = 400
        private val bufferPoolSize = 8

        @BeforeEach
        fun setUp() {
            val tempDirFile = tempDir.toFile()
            fileManager = FileManager(tempDirFile, blockSize)
            val logFile = "txtest.log"
            logManager = LogManager(fileManager, logFile)
            bufferManager = BufferManager(fileManager, logManager, bufferPoolSize)
        }

        @AfterEach
        fun tearDown() {
            // Clean up any test files
            val files = tempDir.toFile().listFiles() ?: return
            for (file in files) {
                file.delete()
            }
        }

        @Test
        fun `should perform basic transaction operations`() {
            // Create transaction 1, write initial values and commit
            val tx1 = Transaction(fileManager, logManager, bufferManager)
            val blk = BlockId(testFile, 1)
            tx1.pin(blk)

            // Set initial values (without logging first time)
            tx1.setInt(blk, 80, 1, false)
            tx1.setString(blk, 40, "one", false)
            tx1.commit()

            // Create transaction 2, read values, modify them, and commit
            val tx2 = Transaction(fileManager, logManager, bufferManager)
            tx2.pin(blk)

            // Read initial values
            val ival = tx2.getInt(blk, 80)
            val sval = tx2.getString(blk, 40)

            // Check initial values
            assertEquals(1, ival)
            assertEquals("one", sval)

            // Modify values
            val newIval = ival + 1
            val newSval = sval + "!"
            tx2.setInt(blk, 80, newIval, true)
            tx2.setString(blk, 40, newSval, true)
            tx2.commit()

            // Create transaction 3, read modified values
            val tx3 = Transaction(fileManager, logManager, bufferManager)
            tx3.pin(blk)

            // Check modified values
            assertEquals(2, tx3.getInt(blk, 80))
            assertEquals("one!", tx3.getString(blk, 40))

            // Modify value but then rollback
            tx3.setInt(blk, 80, 9999, true)
            assertEquals(9999, tx3.getInt(blk, 80))
            tx3.rollback()

            // Create transaction 4, check that rollback worked
            val tx4 = Transaction(fileManager, logManager, bufferManager)
            tx4.pin(blk)

            // Value should be back to what it was before tx3
            assertEquals(2, tx4.getInt(blk, 80))
            tx4.commit()
        }
    }

    @Nested
    inner class MultipleBlocksTest {
        private lateinit var fileManager: FileManager
        private lateinit var logManager: LogManager
        private lateinit var bufferManager: BufferManager
        private val testFile = "testfile"
        private val blockSize = 400
        private val bufferPoolSize = 8

        @BeforeEach
        fun setUp() {
            val tempDirFile = tempDir.toFile()
            fileManager = FileManager(tempDirFile, blockSize)
            val logFile = "txtest.log"
            logManager = LogManager(fileManager, logFile)
            bufferManager = BufferManager(fileManager, logManager, bufferPoolSize)
        }

        @AfterEach
        fun tearDown() {
            // Clean up any test files
            val files = tempDir.toFile().listFiles() ?: return
            for (file in files) {
                file.delete()
            }
        }

        @Test
        fun `should handle multiple blocks in transaction`() {
            // Create transaction 1
            val tx1 = Transaction(fileManager, logManager, bufferManager)

            // Create and pin two blocks
            val blk1 = BlockId(testFile, 1)
            val blk2 = BlockId(testFile, 2)
            tx1.pin(blk1)
            tx1.pin(blk2)

            // Write different values to the two blocks
            tx1.setInt(blk1, 10, 100, true)
            tx1.setString(blk1, 50, "block1", true)

            tx1.setInt(blk2, 20, 200, true)
            tx1.setString(blk2, 60, "block2", true)

            // Commit the transaction
            tx1.commit()

            // Create transaction 2 to verify values
            val tx2 = Transaction(fileManager, logManager, bufferManager)
            tx2.pin(blk1)
            tx2.pin(blk2)

            // Check values in block 1
            assertEquals(100, tx2.getInt(blk1, 10))
            assertEquals("block1", tx2.getString(blk1, 50))

            // Check values in block 2
            assertEquals(200, tx2.getInt(blk2, 20))
            assertEquals("block2", tx2.getString(blk2, 60))

            // Modify one block and rollback
            tx2.setInt(blk1, 10, 999, true)
            tx2.setString(blk2, 60, "modified", true)
            tx2.rollback()

            // Create transaction 3 to verify rollback
            val tx3 = Transaction(fileManager, logManager, bufferManager)
            tx3.pin(blk1)
            tx3.pin(blk2)

            // Values should be the same as after tx1
            assertEquals(100, tx3.getInt(blk1, 10))
            assertEquals("block1", tx3.getString(blk1, 50))
            assertEquals(200, tx3.getInt(blk2, 20))
            assertEquals("block2", tx3.getString(blk2, 60))

            tx3.commit()
        }
    }

    @Nested
    inner class FileOperationsTest {
        private lateinit var fileManager: FileManager
        private lateinit var logManager: LogManager
        private lateinit var bufferManager: BufferManager
        private val testFile = "testfile"
        private val blockSize = 400
        private val bufferPoolSize = 8

        @BeforeEach
        fun setUp() {
            val tempDirFile = tempDir.toFile()
            fileManager = FileManager(tempDirFile, blockSize)
            val logFile = "txtest.log"
            logManager = LogManager(fileManager, logFile)
            bufferManager = BufferManager(fileManager, logManager, bufferPoolSize)
        }

        @AfterEach
        fun tearDown() {
            // Clean up any test files
            val files = tempDir.toFile().listFiles() ?: return
            for (file in files) {
                file.delete()
            }
        }

        @Test
        fun `should handle file operations`() {
            // Create transaction
            val tx = Transaction(fileManager, logManager, bufferManager)

            // Append blocks
            val blk1 = tx.append(testFile)
            val blk2 = tx.append(testFile)
            val blk3 = tx.append(testFile)

            // Check size
            assertEquals(3, tx.size(testFile))

            // Write values to each block
            tx.pin(blk1)
            tx.setInt(blk1, 10, 1, true)

            tx.pin(blk2)
            tx.setInt(blk2, 10, 2, true)

            tx.pin(blk3)
            tx.setInt(blk3, 10, 3, true)

            // Commit
            tx.commit()

            // Create new transaction to verify
            val tx2 = Transaction(fileManager, logManager, bufferManager)

            // Check size again
            assertEquals(3, tx2.size(testFile))

            // Read values from each block
            tx2.pin(blk1)
            assertEquals(1, tx2.getInt(blk1, 10))

            tx2.pin(blk2)
            assertEquals(2, tx2.getInt(blk2, 10))

            tx2.pin(blk3)
            assertEquals(3, tx2.getInt(blk3, 10))

            tx2.commit()
        }
    }
}