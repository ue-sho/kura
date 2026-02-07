package com.kura.materialize

import com.kura.plan.Plan
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MergeJoinPlanTest {
    private lateinit var transaction: Transaction

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        every { transaction.blockSize() } returns 400
    }

    @Test
    fun `recordsOutput should use merge join formula`() {
        // Arrange
        val schema1 = Schema()
        schema1.addIntField("a")
        val schema2 = Schema()
        schema2.addIntField("b")

        val p1 = mockk<Plan>(relaxed = true)
        val p2 = mockk<Plan>(relaxed = true)
        every { p1.schema() } returns schema1
        every { p2.schema() } returns schema2
        every { p1.recordsOutput() } returns 100
        every { p2.recordsOutput() } returns 200
        every { p1.distinctValues("a") } returns 10
        every { p2.distinctValues("b") } returns 20

        val joinPlan = MergeJoinPlan(transaction, p1, p2, "a", "b")

        // Act
        // R(p1)*R(p2)/max(V(p1,a),V(p2,b)) = 100*200/max(10,20) = 20000/20 = 1000
        val result = joinPlan.recordsOutput()

        // Assert
        assertEquals(1000, result)
    }

    @Test
    fun `blocksAccessed should sum both sorted plan accesses`() {
        // Arrange
        val schema1 = Schema()
        schema1.addIntField("a")
        val schema2 = Schema()
        schema2.addIntField("b")

        val p1 = mockk<Plan>(relaxed = true)
        val p2 = mockk<Plan>(relaxed = true)
        every { p1.schema() } returns schema1
        every { p2.schema() } returns schema2
        every { p1.recordsOutput() } returns 100
        every { p2.recordsOutput() } returns 200

        val joinPlan = MergeJoinPlan(transaction, p1, p2, "a", "b")

        // Act
        val blocks = joinPlan.blocksAccessed()

        // Assert
        assertTrue(blocks > 0)
    }

    @Test
    fun `schema should combine both plan schemas`() {
        // Arrange
        val schema1 = Schema()
        schema1.addIntField("a")
        val schema2 = Schema()
        schema2.addIntField("b")

        val p1 = mockk<Plan>(relaxed = true)
        val p2 = mockk<Plan>(relaxed = true)
        every { p1.schema() } returns schema1
        every { p2.schema() } returns schema2

        val joinPlan = MergeJoinPlan(transaction, p1, p2, "a", "b")

        // Act & Assert
        assertTrue(joinPlan.schema().hasField("a"))
        assertTrue(joinPlan.schema().hasField("b"))
    }

    @Test
    fun `distinctValues should delegate to appropriate plan`() {
        // Arrange
        val schema1 = Schema()
        schema1.addIntField("a")
        val schema2 = Schema()
        schema2.addIntField("b")

        val p1 = mockk<Plan>(relaxed = true)
        val p2 = mockk<Plan>(relaxed = true)
        every { p1.schema() } returns schema1
        every { p2.schema() } returns schema2
        every { p1.distinctValues("a") } returns 10
        every { p2.distinctValues("b") } returns 20

        val joinPlan = MergeJoinPlan(transaction, p1, p2, "a", "b")

        // Act & Assert
        assertEquals(10, joinPlan.distinctValues("a"))
        assertEquals(20, joinPlan.distinctValues("b"))
    }
}
