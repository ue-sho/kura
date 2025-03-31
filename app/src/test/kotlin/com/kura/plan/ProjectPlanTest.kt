package com.kura.plan

import com.kura.query.ProjectScan
import com.kura.query.Scan
import com.kura.record.Schema
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ProjectPlanTest {
    private lateinit var plan: Plan
    private lateinit var scan: Scan
    private lateinit var planSchema: Schema
    private lateinit var projectPlan: ProjectPlan
    private val fieldList = listOf("field1", "field2", "field3")

    @BeforeEach
    fun setUp() {
        // Arrange
        plan = mockk(relaxed = true)
        scan = mockk(relaxed = true)
        planSchema = mockk(relaxed = true)

        every { plan.open() } returns scan
        every { plan.schema() } returns planSchema
        every { planSchema.fields() } returns fieldList

        // Mock a real Schema for testing
        // Since add method is called during schema initialization,
        // creating a mock to spy on that method
        val testSchema = mockk<Schema>(relaxed = true)
        mockkConstructor(Schema::class)
        every { anyConstructed<Schema>().add(any(), any()) } just Runs

        projectPlan = ProjectPlan(plan, fieldList)
    }

    @AfterEach
    fun tearDown() {
        // Clear all mocks after test
        unmockkAll()
    }

    @Test
    fun `open should return a ProjectScan`() {
        // Arrange
        // Already set up in setUp()

        // Act
        val result = projectPlan.open()

        // Assert
        verify { plan.open() }
        assert(result is ProjectScan)
    }

    @Test
    fun `blocksAccessed should return underlying plan blocks accessed`() {
        // Arrange
        val expectedBlocks = 42
        every { plan.blocksAccessed() } returns expectedBlocks

        // Act
        val result = projectPlan.blocksAccessed()

        // Assert
        assertEquals(expectedBlocks, result)
        verify { plan.blocksAccessed() }
    }

    @Test
    fun `recordsOutput should return underlying plan records output`() {
        // Arrange
        val expectedRecords = 100
        every { plan.recordsOutput() } returns expectedRecords

        // Act
        val result = projectPlan.recordsOutput()

        // Assert
        assertEquals(expectedRecords, result)
        verify { plan.recordsOutput() }
    }

    @Test
    fun `distinctValues should return underlying plan distinct values`() {
        // Arrange
        val fieldName = "field1"
        val expectedDistinctValues = 25
        every { plan.distinctValues(fieldName) } returns expectedDistinctValues

        // Act
        val result = projectPlan.distinctValues(fieldName)

        // Assert
        assertEquals(expectedDistinctValues, result)
        verify { plan.distinctValues(fieldName) }
    }

    @Test
    fun `schema should contain only fields in field list`() {
        // ProjectPlan calls schema.add() during initialization,
        // verify that initialization succeeded

        // Arrange: use spy to verify the mock Schema.add call
        val constructedSchema = mockk<Schema>(relaxed = true)
        val schemaSlot = slot<Schema>()

        // Act & Assert: ProjectPlan's initialization, verify that each field is added
        for (field in fieldList) {
            verify { anyConstructed<Schema>().add(field, planSchema) }
        }
    }
}