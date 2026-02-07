package com.kura.materialize

import com.kura.plan.Plan
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GroupByPlanTest {
    private lateinit var transaction: Transaction

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        every { transaction.blockSize() } returns 400
    }

    @Test
    fun `schema should contain group fields and aggregation fields`() {
        // Arrange
        val schema = Schema()
        schema.addStringField("dept", 10)
        schema.addIntField("salary")

        val srcPlan = mockk<Plan>(relaxed = true)
        every { srcPlan.schema() } returns schema

        val countFn = CountFn("salary")
        val maxFn = MaxFn("salary")
        val groupByPlan = GroupByPlan(
            transaction, srcPlan, listOf("dept"), listOf(countFn, maxFn)
        )

        // Act
        val resultSchema = groupByPlan.schema()

        // Assert
        assertTrue(resultSchema.hasField("dept"))
        assertTrue(resultSchema.hasField("countofsalary"))
        assertTrue(resultSchema.hasField("maxofsalary"))
    }

    @Test
    fun `recordsOutput should be product of distinct values of group fields`() {
        // Arrange
        val schema = Schema()
        schema.addStringField("dept", 10)
        schema.addIntField("salary")

        val srcPlan = mockk<Plan>(relaxed = true)
        every { srcPlan.schema() } returns schema
        every { srcPlan.distinctValues("dept") } returns 5

        val countFn = CountFn("salary")
        val groupByPlan = GroupByPlan(
            transaction, srcPlan, listOf("dept"), listOf(countFn)
        )

        // Act & Assert
        assertEquals(5, groupByPlan.recordsOutput())
    }

    @Test
    fun `recordsOutput should multiply distinct values for multiple group fields`() {
        // Arrange
        val schema = Schema()
        schema.addStringField("dept", 10)
        schema.addStringField("city", 10)
        schema.addIntField("salary")

        val srcPlan = mockk<Plan>(relaxed = true)
        every { srcPlan.schema() } returns schema
        every { srcPlan.distinctValues("dept") } returns 5
        every { srcPlan.distinctValues("city") } returns 3

        val countFn = CountFn("salary")
        val groupByPlan = GroupByPlan(
            transaction, srcPlan, listOf("dept", "city"), listOf(countFn)
        )

        // Act & Assert
        assertEquals(15, groupByPlan.recordsOutput())
    }

    @Test
    fun `distinctValues for group field should delegate to underlying plan`() {
        // Arrange
        val schema = Schema()
        schema.addStringField("dept", 10)
        schema.addIntField("salary")

        val srcPlan = mockk<Plan>(relaxed = true)
        every { srcPlan.schema() } returns schema
        every { srcPlan.distinctValues("dept") } returns 5

        val countFn = CountFn("salary")
        val groupByPlan = GroupByPlan(
            transaction, srcPlan, listOf("dept"), listOf(countFn)
        )

        // Act & Assert
        assertEquals(5, groupByPlan.distinctValues("dept"))
    }

    @Test
    fun `distinctValues for aggregate field should return recordsOutput`() {
        // Arrange
        val schema = Schema()
        schema.addStringField("dept", 10)
        schema.addIntField("salary")

        val srcPlan = mockk<Plan>(relaxed = true)
        every { srcPlan.schema() } returns schema
        every { srcPlan.distinctValues("dept") } returns 5

        val countFn = CountFn("salary")
        val groupByPlan = GroupByPlan(
            transaction, srcPlan, listOf("dept"), listOf(countFn)
        )

        // Act & Assert - aggregate field's distinct values = recordsOutput
        assertEquals(5, groupByPlan.distinctValues("countofsalary"))
    }
}
