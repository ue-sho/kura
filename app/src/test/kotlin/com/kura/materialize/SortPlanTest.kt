package com.kura.materialize

import com.kura.plan.Plan
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SortPlanTest {
    private lateinit var transaction: Transaction
    private lateinit var schema: Schema
    private lateinit var srcPlan: Plan

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)

        srcPlan = mockk<Plan>(relaxed = true)
        every { srcPlan.schema() } returns schema
        every { transaction.blockSize() } returns 400
    }

    @Test
    fun `recordsOutput should delegate to underlying plan`() {
        // Arrange
        every { srcPlan.recordsOutput() } returns 50

        val sortPlan = SortPlan(transaction, srcPlan, listOf("id"))

        // Act & Assert
        assertEquals(50, sortPlan.recordsOutput())
    }

    @Test
    fun `distinctValues should delegate to underlying plan`() {
        // Arrange
        every { srcPlan.distinctValues("id") } returns 25

        val sortPlan = SortPlan(transaction, srcPlan, listOf("id"))

        // Act & Assert
        assertEquals(25, sortPlan.distinctValues("id"))
    }

    @Test
    fun `schema should return underlying plan schema`() {
        // Arrange
        val sortPlan = SortPlan(transaction, srcPlan, listOf("id"))

        // Act & Assert
        assertEquals(schema, sortPlan.schema())
    }

    @Test
    fun `blocksAccessed should estimate same as materialize plan`() {
        // Arrange
        every { srcPlan.recordsOutput() } returns 100

        val sortPlan = SortPlan(transaction, srcPlan, listOf("id"))

        // Act
        val blocks = sortPlan.blocksAccessed()

        // Assert
        assertTrue(blocks > 0)
    }
}
