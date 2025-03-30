package com.kura.record

import com.kura.file.BlockId
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Types

class RecordPageTest {
    private lateinit var transaction: Transaction
    private lateinit var blockId: BlockId
    private lateinit var layout: Layout
    private lateinit var schema: Schema
    private lateinit var recordPage: RecordPage

    @BeforeEach
    fun setUp() {
        // Arrange
        transaction = mockk(relaxed = true)
        blockId = BlockId("testfile", 1)
        schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        layout = Layout(schema)
        every { transaction.blockSize() } returns 400

        recordPage = RecordPage(transaction, blockId, layout)
    }

    @Test
    fun `should pin block when creating record page`() {
        // Assert
        verify { transaction.pin(blockId) }
    }

    @Test
    fun `should format record page`() {
        // Act
        recordPage.format()

        // Assert
        verify {
            transaction.setInt(blockId, any(), RecordPage.EMPTY, false)
            transaction.setInt(blockId, any(), 0, false)
            transaction.setString(blockId, any(), "", false)
        }
    }

    @Test
    fun `should read and write int values`() {
        // Arrange
        val slot = 3
        val fieldName = "id"
        val value = 42
        val offset = layout.offset(fieldName)
        val position = slot * layout.slotSize() + offset

        every { transaction.getInt(blockId, position) } returns value

        // Act
        recordPage.setInt(slot, fieldName, value)
        val result = recordPage.getInt(slot, fieldName)

        // Assert
        verify { transaction.setInt(blockId, position, value, true) }
        assertEquals(value, result)
    }

    @Test
    fun `should read and write string values`() {
        // Arrange
        val slot = 3
        val fieldName = "name"
        val value = "test"
        val offset = layout.offset(fieldName)
        val position = slot * layout.slotSize() + offset

        every { transaction.getString(blockId, position) } returns value

        // Act
        recordPage.setString(slot, fieldName, value)
        val result = recordPage.getString(slot, fieldName)

        // Assert
        verify { transaction.setString(blockId, position, value, true) }
        assertEquals(value, result)
    }

    @Test
    fun `should delete record`() {
        // Arrange
        val slot = 3
        val position = slot * layout.slotSize()

        // Act
        recordPage.delete(slot)

        // Assert
        verify { transaction.setInt(blockId, position, RecordPage.EMPTY, true) }
    }

    @Test
    fun `should find next used slot`() {
        // Arrange
        val currentSlot = 3
        val nextSlot = 5

        for (i in (currentSlot + 1)..nextSlot) {
            val flag = if (i == nextSlot) RecordPage.USED else RecordPage.EMPTY
            every { transaction.getInt(blockId, i * layout.slotSize()) } returns flag
        }

        // Act
        val result = recordPage.nextAfter(currentSlot)

        // Assert
        assertEquals(nextSlot, result)
    }

    @Test
    fun `should return -1 when no next used slot exists`() {
        // Arrange
        val currentSlot = 3
        val maxSlots = 10

        for (i in (currentSlot + 1)..maxSlots) {
            every { transaction.getInt(blockId, i * layout.slotSize()) } returns RecordPage.EMPTY
        }

        // Act
        val result = recordPage.nextAfter(currentSlot)

        // Assert
        assertEquals(-1, result)
    }

    @Test
    fun `should insert after current slot`() {
        // Arrange
        val currentSlot = 3
        val emptySlot = 5

        for (i in (currentSlot + 1)..emptySlot) {
            val flag = if (i == emptySlot) RecordPage.EMPTY else RecordPage.USED
            every { transaction.getInt(blockId, i * layout.slotSize()) } returns flag
        }

        // Act
        val result = recordPage.insertAfter(currentSlot)

        // Assert
        assertEquals(emptySlot, result)
        verify { transaction.setInt(blockId, emptySlot * layout.slotSize(), RecordPage.USED, true) }
    }

    @Test
    fun `should return block id`() {
        // Act
        val result = recordPage.block()

        // Assert
        assertEquals(blockId, result)
    }
}