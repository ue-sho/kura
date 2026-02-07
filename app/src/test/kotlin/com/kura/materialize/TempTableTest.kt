package com.kura.materialize

import com.kura.file.BlockId
import com.kura.record.Layout
import com.kura.record.RecordPage
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TempTableTest {
    private lateinit var transaction: Transaction
    private lateinit var schema: Schema

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)

        every { transaction.blockSize() } returns 400
    }

    @Test
    fun `should create temp table with auto-generated name`() {
        // Arrange & Act
        val tempTable = TempTable(transaction, schema)

        // Assert
        assertTrue(tempTable.tableName().startsWith("temp"))
    }

    @Test
    fun `should return layout with correct schema`() {
        // Arrange & Act
        val tempTable = TempTable(transaction, schema)

        // Assert
        val layout = tempTable.getLayout()
        assertTrue(layout.schema().hasField("id"))
        assertTrue(layout.schema().hasField("name"))
    }

    @Test
    fun `should open table scan for temp table`() {
        // Arrange
        val tempTable = TempTable(transaction, schema)
        val filename = tempTable.tableName() + ".tbl"
        every { transaction.size(filename) } returns 0
        every { transaction.append(filename) } returns BlockId(filename, 0)

        // Act
        val scan = tempTable.open()

        // Assert
        assertNotNull(scan)
        scan.close()
    }

    @Test
    fun `should generate unique table names`() {
        // Arrange & Act
        val temp1 = TempTable(transaction, schema)
        val temp2 = TempTable(transaction, schema)

        // Assert
        assertNotEquals(temp1.tableName(), temp2.tableName())
    }
}
