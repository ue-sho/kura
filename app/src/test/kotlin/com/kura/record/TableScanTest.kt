package com.kura.record

import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Types

class TableScanTest {
    private lateinit var transaction: Transaction
    private lateinit var schema: Schema
    private lateinit var layout: Layout
    private lateinit var tableScan: TableScan
    private val tableName = "test_table"
    private val filename = "$tableName.tbl"

    @BeforeEach
    fun setUp() {
        // Arrange
        transaction = mockk(relaxed = true)
        schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        layout = Layout(schema)

        // Mock transaction.size to simulate an existing table with one block
        every { transaction.size(filename) } returns 1
        every { transaction.blockSize() } returns 400

        // Mock blockId for the first block
        val blockId = BlockId(filename, 0)

        // Mock behavior for first slot to be USED when checking records
        val slotZeroPosition = 0 * layout.slotSize()
        every { transaction.getInt(blockId, slotZeroPosition) } returns RecordPage.USED

        tableScan = TableScan(transaction, tableName, layout)
    }

    @Test
    fun `should create new block when table is empty`() {
        // Arrange
        every { transaction.size(filename) } returns 0
        val blockId = BlockId(filename, 0)

        // Act
        val newTableScan = TableScan(transaction, tableName, layout)

        // Assert
        verify {
            transaction.append(filename)
            transaction.setInt(any(), any(), RecordPage.EMPTY, false)
        }
    }

    @Test
    fun `should read and write int values`() {
        // Arrange
        val fieldName = "id"
        val value = 42
        val slot = 0

        // Setup mocks to simulate a record at slot 0
        val blockId = BlockId(filename, 0)
        val slotPosition = slot * layout.slotSize()
        val recordPosition = slotPosition + layout.offset(fieldName)

        every { transaction.getInt(blockId, slotPosition) } returns RecordPage.USED
        every { transaction.getInt(blockId, recordPosition) } returns value

        // Act - simulate moving to the first record
        tableScan.beforeFirst()
        val hasNext = tableScan.next()
        tableScan.setInt(fieldName, value)
        val result = tableScan.getInt(fieldName)

        // Assert
        assertTrue(hasNext)
        assertEquals(value, result)
        verify { transaction.setInt(blockId, recordPosition, value, true) }
    }

    @Test
    fun `should read and write string values`() {
        // Arrange
        val fieldName = "name"
        val value = "test"
        val slot = 0

        // Setup mocks to simulate a record at slot 0
        val blockId = BlockId(filename, 0)
        val slotPosition = slot * layout.slotSize()
        val recordPosition = slotPosition + layout.offset(fieldName)

        every { transaction.getInt(blockId, slotPosition) } returns RecordPage.USED
        every { transaction.getString(blockId, recordPosition) } returns value

        // Act - simulate moving to the first record
        tableScan.beforeFirst()
        val hasNext = tableScan.next()
        tableScan.setString(fieldName, value)
        val result = tableScan.getString(fieldName)

        // Assert
        assertTrue(hasNext)
        assertEquals(value, result)
        verify { transaction.setString(blockId, recordPosition, value, true) }
    }

    @Test
    fun `should insert new record`() {
        // Arrange
        val blockId = BlockId(filename, 0)

        // Mock behavior to indicate no empty slots in current block
        // We need to mock the behavior for the first slot as USED (for initialization)
        // but then all other slots as USED to force a new block creation
        every { transaction.getInt(blockId, 0) } returns RecordPage.USED
        every { transaction.getInt(blockId, any()) } answers {
            if (secondArg<Int>() == 0) RecordPage.USED else RecordPage.USED
        }

        // Mock behavior for a new block
        every { transaction.size(filename) } returns 1
        every { transaction.append(filename) } returns BlockId(filename, 1)

        // Act
        tableScan.insert()

        // Assert
        verify {
            transaction.append(filename)
            transaction.setInt(any(), any(), RecordPage.USED, true)
        }
    }

    @Test
    fun `should delete current record`() {
        // Arrange
        val slot = 0
        val blockId = BlockId(filename, 0)
        val slotPosition = slot * layout.slotSize()

        // Setup mocks to simulate a record at slot 0
        every { transaction.getInt(blockId, slotPosition) } returns RecordPage.USED

        // Act - simulate moving to the first record and deleting it
        tableScan.beforeFirst()
        val hasNext = tableScan.next()
        tableScan.delete()

        // Assert
        assertTrue(hasNext)
        verify { transaction.setInt(blockId, slotPosition, RecordPage.EMPTY, true) }
    }

    @Test
    fun `should move to record by record id`() {
        // Arrange
        val blockNum = 1
        val slot = 3
        val recordId = RecordId(blockNum, slot)
        val blockId = BlockId(filename, blockNum)

        // Act
        tableScan.moveToRecordId(recordId)

        // Assert
        verify {
            transaction.pin(blockId)
        }
    }

    @Test
    fun `should get record id of current record`() {
        // Arrange
        val slot = 0
        val blockNum = 0
        val blockId = BlockId(filename, blockNum)
        val slotPosition = slot * layout.slotSize()

        // Setup mocks to simulate a record at slot 0
        every { transaction.getInt(blockId, slotPosition) } returns RecordPage.USED

        // Act - simulate moving to the first record
        tableScan.beforeFirst()
        val hasNext = tableScan.next()
        val recordId = tableScan.getRecordId()

        // Assert
        assertTrue(hasNext)
        assertEquals(blockNum, recordId.blockNumber())
        assertEquals(slot, recordId.slot())
    }

    @Test
    fun `should close and unpin block`() {
        // Act
        tableScan.close()

        // Assert
        verify { transaction.unpin(any()) }
    }

    @Test
    fun `should handle constant values`() {
        // Arrange
        val intFieldName = "id"
        val stringFieldName = "name"
        val intValue = 42
        val stringValue = "test"
        val intConstant = Constant(intValue)
        val stringConstant = Constant(stringValue)
        val slot = 0

        // We need to first make sure there's a current record in the scan
        val blockId = BlockId(filename, 0)
        val slotPosition = slot * layout.slotSize()
        every { transaction.getInt(blockId, slotPosition) } returns RecordPage.USED

        // Move to the first record
        tableScan.beforeFirst()
        tableScan.next()

        // Now prepare the field positions for the current slot
        val intFieldPosition = slotPosition + layout.offset(intFieldName)
        val stringFieldPosition = slotPosition + layout.offset(stringFieldName)

        // Setup mock responses for field values
        every { transaction.getInt(blockId, intFieldPosition) } returns intValue
        every { transaction.getString(blockId, stringFieldPosition) } returns stringValue

        // Act & Assert - setVal and getVal for int field
        tableScan.setVal(intFieldName, intConstant)
        val retrievedIntConstant = tableScan.getVal(intFieldName)

        // Assert
        assertEquals(intValue, retrievedIntConstant.asInt())
        verify { transaction.setInt(blockId, intFieldPosition, intValue, true) }

        // Act & Assert - setVal and getVal for string field
        tableScan.setVal(stringFieldName, stringConstant)
        val retrievedStringConstant = tableScan.getVal(stringFieldName)

        // Assert
        assertEquals(stringValue, retrievedStringConstant.asString())
        verify { transaction.setString(blockId, stringFieldPosition, stringValue, true) }
    }
}