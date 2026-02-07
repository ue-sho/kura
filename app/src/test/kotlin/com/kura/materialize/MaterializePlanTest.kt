package com.kura.materialize

import com.kura.plan.Plan
import com.kura.query.Constant
import com.kura.query.Scan
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MaterializePlanTest {
    private lateinit var transaction: Transaction
    private lateinit var srcPlan: Plan
    private lateinit var srcScan: Scan
    private lateinit var schema: Schema

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        srcPlan = mockk(relaxed = true)
        srcScan = mockk(relaxed = true)
        schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)

        every { srcPlan.schema() } returns schema
        every { srcPlan.open() } returns srcScan
        every { transaction.blockSize() } returns 400
    }

    @Test
    fun `should materialize records from source plan`() {
        // Arrange
        // Simulate 2 records
        every { srcScan.next() } returnsMany listOf(true, true, false)
        every { srcScan.getVal("id") } returnsMany listOf(Constant(1), Constant(2))
        every { srcScan.getVal("name") } returnsMany listOf(Constant("alice"), Constant("bob"))

        val materializePlan = MaterializePlan(transaction, srcPlan)

        // Act
        val scan = materializePlan.open()

        // Assert
        verify { srcScan.close() }
        assertNotNull(scan)
        scan.close()
    }

    @Test
    fun `blocksAccessed should estimate based on record size`() {
        // Arrange
        every { srcPlan.recordsOutput() } returns 100

        val materializePlan = MaterializePlan(transaction, srcPlan)

        // Act
        val blocks = materializePlan.blocksAccessed()

        // Assert
        assertTrue(blocks > 0)
    }

    @Test
    fun `recordsOutput should delegate to source plan`() {
        // Arrange
        every { srcPlan.recordsOutput() } returns 42

        val materializePlan = MaterializePlan(transaction, srcPlan)

        // Act & Assert
        assertEquals(42, materializePlan.recordsOutput())
    }

    @Test
    fun `distinctValues should delegate to source plan`() {
        // Arrange
        every { srcPlan.distinctValues("id") } returns 10

        val materializePlan = MaterializePlan(transaction, srcPlan)

        // Act & Assert
        assertEquals(10, materializePlan.distinctValues("id"))
    }

    @Test
    fun `schema should return source plan schema`() {
        // Arrange
        val materializePlan = MaterializePlan(transaction, srcPlan)

        // Act & Assert
        assertEquals(schema, materializePlan.schema())
    }
}
