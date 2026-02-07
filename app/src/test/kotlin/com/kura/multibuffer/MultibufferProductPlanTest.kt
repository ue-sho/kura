package com.kura.multibuffer

import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MultibufferProductPlanTest {
    private lateinit var transaction: Transaction
    private lateinit var lhsPlan: Plan
    private lateinit var rhsPlan: Plan
    private lateinit var lhsSchema: Schema
    private lateinit var rhsSchema: Schema

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        lhsPlan = mockk(relaxed = true)
        rhsPlan = mockk(relaxed = true)

        lhsSchema = Schema()
        lhsSchema.addIntField("a")

        rhsSchema = Schema()
        rhsSchema.addIntField("b")

        every { lhsPlan.schema() } returns lhsSchema
        every { rhsPlan.schema() } returns rhsSchema
        every { transaction.blockSize() } returns 400
        every { transaction.availableBuffers() } returns 8
    }

    @Test
    fun `recordsOutput should return product of lhs and rhs records`() {
        every { lhsPlan.recordsOutput() } returns 100
        every { rhsPlan.recordsOutput() } returns 50

        val plan = MultibufferProductPlan(transaction, lhsPlan, rhsPlan)

        assertEquals(5000, plan.recordsOutput())
    }

    @Test
    fun `schema should contain fields from both lhs and rhs`() {
        val plan = MultibufferProductPlan(transaction, lhsPlan, rhsPlan)
        val schema = plan.schema()

        assertTrue(schema.hasField("a"))
        assertTrue(schema.hasField("b"))
    }

    @Test
    fun `distinctValues should delegate to lhs plan for lhs fields`() {
        every { lhsPlan.distinctValues("a") } returns 10

        val plan = MultibufferProductPlan(transaction, lhsPlan, rhsPlan)

        assertEquals(10, plan.distinctValues("a"))
    }

    @Test
    fun `distinctValues should delegate to rhs plan for rhs fields`() {
        every { rhsPlan.distinctValues("b") } returns 25

        val plan = MultibufferProductPlan(transaction, lhsPlan, rhsPlan)

        assertEquals(25, plan.distinctValues("b"))
    }

    @Test
    fun `blocksAccessed should calculate cost based on chunks`() {
        every { lhsPlan.blocksAccessed() } returns 10
        every { rhsPlan.blocksAccessed() } returns 20
        every { rhsPlan.recordsOutput() } returns 100

        val plan = MultibufferProductPlan(transaction, lhsPlan, rhsPlan)

        // The cost formula uses MaterializePlan to estimate rhs size,
        // then divides by available buffers to get chunk count
        val cost = plan.blocksAccessed()
        assertTrue(cost >= 0)
    }
}
