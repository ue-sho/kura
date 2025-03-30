package com.kura.record

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.Types

class LayoutTest {

    @Test
    fun `should create layout from schema`() {
        // Arrange
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)

        // Act
        val layout = Layout(schema)

        // Assert
        assertSame(schema, layout.schema())
        assertTrue(layout.slotSize() > 0)
    }

    @Test
    fun `should calculate correct offsets for fields`() {
        // Arrange
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        schema.addIntField("age")

        // Act
        val layout = Layout(schema)

        // Assert
        val flagSize = Integer.SIZE / 8 // Empty/inuse flag size in bytes
        assertEquals(flagSize, layout.offset("id"))
        assertEquals(flagSize + Integer.SIZE / 8, layout.offset("name"))

        // The string field's offset should be after id's offset and its size
        val stringFieldOffset = layout.offset("name")
        assertTrue(stringFieldOffset > layout.offset("id"))

        // The age field's offset should be after name's offset and its size
        val ageFieldOffset = layout.offset("age")
        assertTrue(ageFieldOffset > stringFieldOffset)
    }

    @Test
    fun `should calculate correct slot size`() {
        // Arrange
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)

        // Act
        val layout = Layout(schema)

        // Assert
        val expectedSlotSize = Integer.SIZE / 8 // flag
                + Integer.SIZE / 8 // id field
                + (10 * Character.SIZE / 8) + Integer.SIZE / 8 // name field with length
        assertTrue(layout.slotSize() >= expectedSlotSize)
    }

    @Test
    fun `should create layout from existing metadata`() {
        // Arrange
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)

        val offsets = mapOf(
            "id" to 4,
            "name" to 8
        )
        val slotSize = 50

        // Act
        val layout = Layout(schema, offsets, slotSize)

        // Assert
        assertSame(schema, layout.schema())
        assertEquals(4, layout.offset("id"))
        assertEquals(8, layout.offset("name"))
        assertEquals(50, layout.slotSize())
    }
}