package com.kura.opt

import com.kura.metadata.IndexInfo
import com.kura.metadata.MetadataManager
import com.kura.metadata.StatisticsInfo
import com.kura.plan.Plan
import com.kura.query.Constant
import com.kura.query.Predicate
import com.kura.query.Term
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TablePlannerTest {
    private lateinit var transaction: Transaction
    private lateinit var metadataManager: MetadataManager

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        metadataManager = mockk(relaxed = true)

        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 20)
        val layout = Layout(schema)
        val stats = StatisticsInfo(10, 100)

        every { metadataManager.getLayout("t1", transaction) } returns layout
        every { metadataManager.getStatisticsInfo("t1", any(), transaction) } returns stats
        every { metadataManager.getIndexInfo("t1", transaction) } returns emptyMap()
        every { transaction.blockSize() } returns 400
        every { transaction.availableBuffers() } returns 8
    }

    @Test
    fun `makeSelectPlan should return plan for table without index`() {
        val predicate = Predicate()
        val tp = TablePlanner("t1", predicate, transaction, metadataManager)
        val plan = tp.makeSelectPlan()
        assertNotNull(plan)
        assertTrue(plan.schema().hasField("id"))
        assertTrue(plan.schema().hasField("name"))
    }

    @Test
    fun `makeSelectPlan should use index when predicate equates field with constant`() {
        val indexInfo = mockk<IndexInfo>(relaxed = true)
        every { metadataManager.getIndexInfo("t1", transaction) } returns mapOf("id" to indexInfo)
        every { indexInfo.recordsOutput() } returns 1
        every { indexInfo.blocksAccessed() } returns 1

        val predicate = mockk<Predicate>(relaxed = true)
        every { predicate.equatesWithConstant("id") } returns Constant(42)
        every { predicate.selectSubPredicate(any()) } returns null

        val tp = TablePlanner("t1", predicate, transaction, metadataManager)
        val plan = tp.makeSelectPlan()
        assertNotNull(plan)
    }

    @Test
    fun `makeProductPlan should return a plan`() {
        val predicate = Predicate()
        val tp = TablePlanner("t1", predicate, transaction, metadataManager)

        val currentPlan = mockk<Plan>(relaxed = true)
        val currentSchema = Schema()
        currentSchema.addIntField("x")
        every { currentPlan.schema() } returns currentSchema
        every { currentPlan.recordsOutput() } returns 50

        val plan = tp.makeProductPlan(currentPlan)
        assertNotNull(plan)
        assertTrue(plan.schema().hasField("id"))
        assertTrue(plan.schema().hasField("x"))
    }

    @Test
    fun `makeJoinPlan should return null when no join predicate exists`() {
        val predicate = Predicate()
        val tp = TablePlanner("t1", predicate, transaction, metadataManager)

        val currentPlan = mockk<Plan>(relaxed = true)
        val currentSchema = Schema()
        currentSchema.addIntField("x")
        every { currentPlan.schema() } returns currentSchema

        val plan = tp.makeJoinPlan(currentPlan)
        assertNull(plan)
    }
}
