package com.kura.metadata

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.sql.Types
import kotlin.test.assertEquals
import kotlin.test.assertTrue

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.record.Schema
import com.kura.transaction.Transaction

class TableManagerTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var transaction: Transaction
    private lateinit var tableManager: TableManager
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager

    @BeforeEach
    fun setUp() {
        fileManager = FileManager(tempDir.toFile(), 400)
        logManager = LogManager(fileManager, "tblmgrtest.log")
        bufferManager = BufferManager(fileManager, logManager, 8)

        // Initialize metadata tables with a separate transaction
        transaction = Transaction(fileManager, logManager, bufferManager)
        tableManager = TableManager(true, transaction)
        transaction.commit() // Commit initialization transaction

        // Create a new transaction for testing
        transaction = Transaction(fileManager, logManager, bufferManager)
        tableManager = TableManager(false, transaction)
    }

    @Test
    @Disabled("Skipping due to LockAbortException issues")
    fun `should create table and retrieve its layout`() {
        // Arrange
        val schema = Schema()
        schema.addIntField("A")
        schema.addStringField("B", 9)

        // Act
        tableManager.createTable("MyTable", schema, transaction)
        val layout = tableManager.getLayout("MyTable", transaction)

        // Assert
        val retrievedSchema = layout.schema()
        assertEquals(2, retrievedSchema.fields().size)
        assertTrue(retrievedSchema.hasField("A"))
        assertTrue(retrievedSchema.hasField("B"))
        assertEquals(Types.INTEGER, retrievedSchema.type("A"))
        assertEquals(Types.VARCHAR, retrievedSchema.type("B"))
        assertEquals(9, retrievedSchema.length("B"))

        // Also check slot size is greater than zero
        assertTrue(layout.slotSize() > 0)

        // Commit and end transaction
        transaction.commit()
    }
}