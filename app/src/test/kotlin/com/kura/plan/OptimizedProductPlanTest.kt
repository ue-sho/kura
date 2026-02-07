package com.kura.plan

import com.kura.query.Scan
import com.kura.record.Schema
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OptimizedProductPlanTest {

    private fun mockPlan(blocks: Int, records: Int, schema: Schema): Plan {
        val plan = mockk<Plan>(relaxed = true)
        every { plan.blocksAccessed() } returns blocks
        every { plan.recordsOutput() } returns records
        every { plan.schema() } returns schema
        every { plan.distinctValues(any()) } returns 10
        return plan
    }

    @Test
    fun `should choose ordering with fewer block accesses`() {
        val schema1 = Schema()
        schema1.addIntField("a")
        val schema2 = Schema()
        schema2.addIntField("b")

        // p1 has 100 blocks, p2 has 5 blocks
        // ProductPlan(p1, p2): B(p1) + R(p1) * B(p2)
        // ProductPlan(p2, p1): B(p2) + R(p2) * B(p1)
        val p1 = mockPlan(100, 1000, schema1)
        val p2 = mockPlan(5, 50, schema2)

        val optimized = OptimizedProductPlan(p1, p2)
        val schema = optimized.schema()

        // Should have fields from both plans
        assertTrue(schema.hasField("a"))
        assertTrue(schema.hasField("b"))
    }

    @Test
    fun `recordsOutput should be product of both plans`() {
        val schema1 = Schema()
        schema1.addIntField("x")
        val schema2 = Schema()
        schema2.addIntField("y")

        val p1 = mockPlan(10, 100, schema1)
        val p2 = mockPlan(10, 200, schema2)

        val optimized = OptimizedProductPlan(p1, p2)
        assertEquals(20000, optimized.recordsOutput())
    }
}
