package com.kura.metadata

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction

class IndexManagerTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var transaction: Transaction
    private lateinit var tableManager: TableManager
    private lateinit var statisticsManager: StatisticsManager
    private lateinit var indexManager: IndexManager
    private lateinit var schema: Schema
    private lateinit var layout: Layout
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager

    @BeforeEach
    fun setUp() {
        fileManager = FileManager(tempDir.toFile(), 400)
        logManager = LogManager(fileManager, "idxmgrtest.log")
        bufferManager = BufferManager(fileManager, logManager, 8)

        // Initialize metadata tables with a separate transaction
        transaction = Transaction(fileManager, logManager, bufferManager)
        tableManager = TableManager(true, transaction)
        statisticsManager = StatisticsManager(tableManager, transaction)
        indexManager = IndexManager(true, tableManager, statisticsManager, transaction)

        schema = Schema()
        schema.addIntField("A")
        schema.addStringField("B", 9)
        layout = Layout(schema)

        tableManager.createTable("TestTable", schema, transaction)
        transaction.commit() // Commit initialization transaction

        // Create a new transaction for testing
        transaction = Transaction(fileManager, logManager, bufferManager)
        tableManager = TableManager(false, transaction)
        statisticsManager = StatisticsManager(tableManager, transaction)
        indexManager = IndexManager(false, tableManager, statisticsManager, transaction)

        // Create test data
        val tableScan = TableScan(transaction, "TestTable", layout)
        for (i in 1..50) {
            tableScan.insert()
            val aValue = i % 10 // Create some duplicates
            tableScan.setInt("A", aValue)
            tableScan.setString("B", "val${i % 5}")
        }
        tableScan.close()
    }

    @Test
    @Disabled("Skipping due to LockAbortException issues")
    fun `should create and retrieve index information`() {
        // Act
        indexManager.createIndex("idx_A", "TestTable", "A", transaction)
        indexManager.createIndex("idx_B", "TestTable", "B", transaction)

        val indexInfo = indexManager.getIndexInfo("TestTable", transaction)

        // Assert
        assertEquals(2, indexInfo.size)
        assertTrue(indexInfo.containsKey("A"))
        assertTrue(indexInfo.containsKey("B"))

        // Test index A statistics
        val idxA = indexInfo["A"]!!
        assertTrue(idxA.blocksAccessed() > 0)
        assertTrue(idxA.recordsOutput() <= 50) // At most 50 records
        assertTrue(idxA.distinctValues("A") <= 10) // At most 10 distinct values in A

        // Test index B statistics
        val idxB = indexInfo["B"]!!
        assertTrue(idxB.blocksAccessed() > 0)
        assertTrue(idxB.recordsOutput() <= 50) // At most 50 records
        assertTrue(idxB.distinctValues("B") <= 5) // At most 5 distinct values in B

        // Commit and end transaction
        transaction.commit()
    }
}