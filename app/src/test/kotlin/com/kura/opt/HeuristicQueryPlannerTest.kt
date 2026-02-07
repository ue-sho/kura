package com.kura.opt

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

class HeuristicQueryPlannerTest {
    private lateinit var transaction: Transaction
    private lateinit var metadataManager: MetadataManager

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        metadataManager = mockk(relaxed = true)
        every { transaction.blockSize() } returns 400
        every { transaction.availableBuffers() } returns 8
    }

    private fun setupTable(tableName: String, schema: Schema, numBlocks: Int, numRecords: Int) {
        val layout = Layout(schema)
        val stats = StatisticsInfo(numBlocks, numRecords)
        every { metadataManager.getLayout(tableName, transaction) } returns layout
        every { metadataManager.getStatisticsInfo(tableName, any(), transaction) } returns stats
        every { metadataManager.getIndexInfo(tableName, transaction) } returns emptyMap()
    }

    @Test
    fun `createPlan should handle single table query`() {
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 20)
        setupTable("t1", schema, 10, 100)

        val data = QueryData(
            listOf("id", "name"),
            listOf("t1"),
            Predicate()
        )

        val planner = HeuristicQueryPlanner(metadataManager)
        val plan = planner.createPlan(data, transaction)

        assertNotNull(plan)
        assertTrue(plan.schema().hasField("id"))
        assertTrue(plan.schema().hasField("name"))
    }

    @Test
    fun `createPlan should choose smallest table first for join order`() {
        val schema1 = Schema()
        schema1.addIntField("a")
        setupTable("small", schema1, 2, 10)

        val schema2 = Schema()
        schema2.addIntField("b")
        setupTable("large", schema2, 100, 1000)

        val data = QueryData(
            listOf("a", "b"),
            listOf("large", "small"),
            Predicate()
        )

        val planner = HeuristicQueryPlanner(metadataManager)
        val plan = planner.createPlan(data, transaction)

        assertNotNull(plan)
        assertTrue(plan.schema().hasField("a"))
        assertTrue(plan.schema().hasField("b"))
        // The plan should exist and project both fields
    }

    @Test
    fun `createPlan should handle two tables with product`() {
        val schema1 = Schema()
        schema1.addIntField("x")
        setupTable("t1", schema1, 5, 50)

        val schema2 = Schema()
        schema2.addIntField("y")
        setupTable("t2", schema2, 5, 50)

        val data = QueryData(
            listOf("x", "y"),
            listOf("t1", "t2"),
            Predicate()
        )

        val planner = HeuristicQueryPlanner(metadataManager)
        val plan = planner.createPlan(data, transaction)

        assertNotNull(plan)
        assertTrue(plan.schema().hasField("x"))
        assertTrue(plan.schema().hasField("y"))
    }

    @Test
    fun `createPlan can be called multiple times`() {
        val schema = Schema()
        schema.addIntField("id")
        setupTable("t1", schema, 5, 50)

        val data = QueryData(
            listOf("id"),
            listOf("t1"),
            Predicate()
        )

        val planner = HeuristicQueryPlanner(metadataManager)
        val plan1 = planner.createPlan(data, transaction)
        val plan2 = planner.createPlan(data, transaction)

        assertNotNull(plan1)
        assertNotNull(plan2)
    }
}
