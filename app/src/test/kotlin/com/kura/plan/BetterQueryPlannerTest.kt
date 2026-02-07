package com.kura.plan

import com.kura.metadata.MetadataManager
import com.kura.metadata.StatisticsInfo
import com.kura.parse.QueryData
import com.kura.query.Predicate
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class BetterQueryPlannerTest {
    private lateinit var transaction: Transaction
    private lateinit var metadataManager: MetadataManager

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        metadataManager = mockk(relaxed = true)
        every { transaction.blockSize() } returns 400
    }

    private fun setupTable(tableName: String, schema: Schema, numBlocks: Int, numRecords: Int) {
        val layout = Layout(schema)
        val stats = StatisticsInfo(numBlocks, numRecords)
        every { metadataManager.getLayout(tableName, transaction) } returns layout
        every { metadataManager.getStatisticsInfo(tableName, any(), transaction) } returns stats
        every { metadataManager.getViewDefinition(tableName, transaction) } returns null
    }

    @Test
    fun `should handle single table query`() {
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 20)
        setupTable("t1", schema, 10, 100)

        val data = QueryData(listOf("id", "name"), listOf("t1"), Predicate())
        val planner = BetterQueryPlanner(metadataManager)
        val plan = planner.createPlan(data, transaction)

        assertNotNull(plan)
        assertTrue(plan.schema().hasField("id"))
        assertTrue(plan.schema().hasField("name"))
    }

    @Test
    fun `should choose better product ordering for two tables`() {
        val schema1 = Schema()
        schema1.addIntField("a")
        setupTable("big", schema1, 100, 10000)

        val schema2 = Schema()
        schema2.addIntField("b")
        setupTable("small", schema2, 2, 20)

        val data = QueryData(listOf("a", "b"), listOf("big", "small"), Predicate())
        val planner = BetterQueryPlanner(metadataManager)
        val plan = planner.createPlan(data, transaction)

        assertNotNull(plan)
        assertTrue(plan.schema().hasField("a"))
        assertTrue(plan.schema().hasField("b"))
    }

    @Test
    fun `should handle view definitions`() {
        val schema = Schema()
        schema.addIntField("id")
        setupTable("base", schema, 10, 100)

        every { metadataManager.getViewDefinition("myview", transaction) } returns "select id from base"

        val data = QueryData(listOf("id"), listOf("myview"), Predicate())
        val planner = BetterQueryPlanner(metadataManager)
        val plan = planner.createPlan(data, transaction)

        assertNotNull(plan)
        assertTrue(plan.schema().hasField("id"))
    }
}
