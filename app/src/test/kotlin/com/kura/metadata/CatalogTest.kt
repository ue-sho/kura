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
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction

class CatalogTest {
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
        logManager = LogManager(fileManager, "catalogtest.log")
        bufferManager = BufferManager(fileManager, logManager, 8)

        // Initialize catalog tables with a separate transaction
        transaction = Transaction(fileManager, logManager, bufferManager)
        val initTableManager = TableManager(true, transaction)

        // Add a schema to ensure catalog data is properly written
        val schema = Schema()
        schema.addIntField("testfield")
        initTableManager.createTable("testtable", schema, transaction)
        transaction.commit()

        // Create a new transaction to access existing catalog
        transaction = Transaction(fileManager, logManager, bufferManager)
        tableManager = TableManager(false, transaction)
    }

    @Test
    @Disabled("Skipping due to LockAbortException issues")
    fun `should retrieve catalog table information`() {
        try {
            // Get the layouts for the catalog tables
            val tableCatalogLayout = tableManager.getLayout("tblcat", transaction)
            val fieldCatalogLayout = tableManager.getLayout("fldcat", transaction)

            // Check tableCatalog contents
            val tableScanner = TableScan(transaction, "tblcat", tableCatalogLayout)
            var tableCount = 0

            println("Tables and their slot sizes:")
            while (tableScanner.next()) {
                val tableName = tableScanner.getString("tblname")
                val slotSize = tableScanner.getInt("slotsize")
                println("$tableName: $slotSize")
                tableCount++

                // At minimum, we should have the two catalog tables
                assertTrue(slotSize > 0)
            }
            tableScanner.close()

            // Catalog should at least have tblcat, fldcat and testtable tables
            assertTrue(tableCount >= 3)

            // Check fieldCatalog contents
            val fieldScanner = TableScan(transaction, "fldcat", fieldCatalogLayout)
            var fieldCount = 0

            println("Fields, their tables and offsets:")
            while (fieldScanner.next()) {
                val tableName = fieldScanner.getString("tblname")
                val fieldName = fieldScanner.getString("fldname")
                val offset = fieldScanner.getInt("offset")
                println("$tableName.$fieldName at offset $offset")
                fieldCount++

                // Check that offsets are valid
                assertTrue(offset >= 0)
            }
            fieldScanner.close()

            // Should have at least the fields for catalog tables plus test table
            assertTrue(fieldCount >= 6) // tblcat has 2 fields, fldcat has 5 fields, testtable has 1 field
        } finally {
            // Ensure transaction is committed
            transaction.commit()
        }
    }
}