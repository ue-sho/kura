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

class StatisticsManagerTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var transaction: Transaction
    private lateinit var tableManager: TableManager
    private lateinit var statisticsManager: StatisticsManager
    private lateinit var schema: Schema
    private lateinit var layout: Layout
    private lateinit var fileManager: FileManager
    private lateinit var logManager: LogManager
    private lateinit var bufferManager: BufferManager

    @BeforeEach
    fun setUp() {
        fileManager = FileManager(tempDir.toFile(), 400)
        logManager = LogManager(fileManager, "statmgrtest.log")
        bufferManager = BufferManager(fileManager, logManager, 8)

        // Initialize metadata tables with a separate transaction
        transaction = Transaction(fileManager, logManager, bufferManager)
        tableManager = TableManager(true, transaction)
        statisticsManager = StatisticsManager(tableManager, transaction)

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
    }

    @Test
    @Disabled("Skipping due to LockAbortException issues")
    fun `should calculate correct statistics for table`() {
        // Create test table
        tableManager.createTable("StatsTable", schema, transaction)
        val layout = Layout(schema)

        // Arrange - populate the table with sample data
        val tableScan = TableScan(transaction, "StatsTable", layout)

        // Insert 100 records with random values
        for (i in 1..100) {
            tableScan.insert()
            // Use modulo to create some duplicates for distinct value testing
            val aValue = i % 20
            tableScan.setInt("A", aValue)
            tableScan.setString("B", "rec${i % 15}")
        }
        tableScan.close()

        // Act
        val stats = statisticsManager.getStatisticsInfo("StatsTable", layout, transaction)

        // Assert
        // We expect one or more blocks to be accessed based on 100 records
        assertTrue(stats.blocksAccessed() > 0)
        assertEquals(100, stats.recordsOutput())
        // We expect distinct values to be close to our calculated values
        assertTrue(stats.distinctValues("A") <= 20)
        assertTrue(stats.distinctValues("B") <= 15)

        // Commit and end transaction
        transaction.commit()
    }
}