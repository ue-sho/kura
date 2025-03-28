package com.kura.file

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BlockIdTest {
    @Test
    fun `test BlockId properties and methods`() {
        // Arrange
        val fileName = "testfile"
        val blockNum = 123
        val blockId = BlockId(fileName, blockNum)

        // Assert
        assertEquals(fileName, blockId.fileName, "fileName should match the constructor argument")
        assertEquals(blockNum, blockId.blockNum, "number should match the constructor argument")
        assertEquals("[file testfile, block 123]", blockId.toString(), "toString should return the correct format")

        // Test hash consistency
        val hash1 = blockId.hashCode()
        val hash2 = blockId.hashCode()
        assertEquals(hash1, hash2, "hashCode should be consistent")

        // Test data class equality
        val sameBlockId = BlockId(fileName, blockNum)
        assertEquals(blockId, sameBlockId, "Equal BlockIds should be equal")
        assertEquals(blockId.hashCode(), sameBlockId.hashCode(), "Equal BlockIds should have the same hash code")
    }
}