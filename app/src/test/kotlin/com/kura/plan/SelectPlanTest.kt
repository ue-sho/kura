package com.kura.plan

import com.kura.query.Constant
import com.kura.query.Predicate
import com.kura.query.Scan
import com.kura.query.SelectScan
import com.kura.record.Schema
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SelectPlanTest {
    private lateinit var plan: Plan
    private lateinit var predicate: Predicate
    private lateinit var scan: Scan
    private lateinit var schema: Schema
    private lateinit var selectPlan: SelectPlan
    private val fieldName = "test_field"

    @BeforeEach
    fun setUp() {
        // Arrange
        plan = mockk(relaxed = true)
        predicate = mockk(relaxed = true)
        scan = mockk(relaxed = true)
        schema = mockk(relaxed = true)

        every { plan.open() } returns scan
        every { plan.schema() } returns schema

        selectPlan = SelectPlan(plan, predicate)
    }

    @Test
    fun `open should return a SelectScan`() {
        // Arrange
        // Already set up in setUp()

        // Act
        val result = selectPlan.open()

        // Assert
        verify { plan.open() }
        assert(result is SelectScan)
    }

    @Test
    fun `blocksAccessed should return underlying plan blocks accessed`() {
        // Arrange
        val expectedBlocks = 42
        every { plan.blocksAccessed() } returns expectedBlocks

        // Act
        val result = selectPlan.blocksAccessed()

        // Assert
        assertEquals(expectedBlocks, result)
        verify { plan.blocksAccessed() }
    }

    @Test
    fun `recordsOutput should apply reduction factor to plan records output`() {
        // Arrange
        val planRecords = 100
        val reductionFactor = 5
        every { plan.recordsOutput() } returns planRecords
        every { predicate.reductionFactor(plan) } returns reductionFactor

        // Act
        val result = selectPlan.recordsOutput()

        // Assert
        assertEquals(planRecords / reductionFactor, result)
        verify { plan.recordsOutput() }
        verify { predicate.reductionFactor(plan) }
    }

    @Test
    fun `distinctValues should return 1 when predicate equates field with constant`() {
        // Arrange
        val constant = Constant("test_value")
        every { predicate.equatesWithConstant(fieldName) } returns constant

        // Act
        val result = selectPlan.distinctValues(fieldName)

        // Assert
        assertEquals(1, result)
        verify { predicate.equatesWithConstant(fieldName) }
    }

    @Test
    fun `distinctValues should return minimum when predicate equates field with another field`() {
        // Arrange
        val otherField = "other_field"
        val field1DistinctValues = 10
        val field2DistinctValues = 5

        every { predicate.equatesWithConstant(fieldName) } returns null
        every { predicate.equatesWithField(fieldName) } returns otherField
        every { plan.distinctValues(fieldName) } returns field1DistinctValues
        every { plan.distinctValues(otherField) } returns field2DistinctValues

        // Act
        val result = selectPlan.distinctValues(fieldName)

        // Assert
        assertEquals(field2DistinctValues, result)
        verify { predicate.equatesWithConstant(fieldName) }
        verify { predicate.equatesWithField(fieldName) }
        verify { plan.distinctValues(fieldName) }
        verify { plan.distinctValues(otherField) }
    }

    @Test
    fun `distinctValues should return plan distinct values when no predicate conditions on field`() {
        // Arrange
        val expectedDistinctValues = 20

        every { predicate.equatesWithConstant(fieldName) } returns null
        every { predicate.equatesWithField(fieldName) } returns null
        every { plan.distinctValues(fieldName) } returns expectedDistinctValues

        // Act
        val result = selectPlan.distinctValues(fieldName)

        // Assert
        assertEquals(expectedDistinctValues, result)
        verify { predicate.equatesWithConstant(fieldName) }
        verify { predicate.equatesWithField(fieldName) }
        verify { plan.distinctValues(fieldName) }
    }

    @Test
    fun `schema should return underlying plan schema`() {
        // Arrange
        // Already set up in setUp()

        // Act
        val result = selectPlan.schema()

        // Assert
        assertEquals(schema, result)
        verify { plan.schema() }
    }
}