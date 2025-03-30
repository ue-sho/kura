package com.kura.record

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class RecordIdTest {

    @Test
    fun `should create record id with block number and slot`() {
        // Arrange
        val blockNumber = 3
        val slot = 7

        // Act
        val recordId = RecordId(blockNumber, slot)

        // Assert
        assertEquals(blockNumber, recordId.blockNumber())
        assertEquals(slot, recordId.slot())
    }

    @Test
    fun `should compare record ids for equality`() {
        // Arrange
        val recordId1 = RecordId(3, 7)
        val recordId2 = RecordId(3, 7)
        val recordId3 = RecordId(3, 8)
        val recordId4 = RecordId(4, 7)

        // Act & Assert
        assertEquals(recordId1, recordId2)
        assertNotEquals(recordId1, recordId3)
        assertNotEquals(recordId1, recordId4)
    }

    @Test
    fun `should generate consistent hash codes`() {
        // Arrange
        val recordId1 = RecordId(3, 7)
        val recordId2 = RecordId(3, 7)

        // Act & Assert
        assertEquals(recordId1.hashCode(), recordId2.hashCode())
    }

    @Test
    fun `should format to string correctly`() {
        // Arrange
        val blockNumber = 3
        val slot = 7
        val recordId = RecordId(blockNumber, slot)

        // Act
        val result = recordId.toString()

        // Assert
        assertEquals("[$blockNumber, $slot]", result)
    }
}