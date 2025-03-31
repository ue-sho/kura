package com.kura.plan

import com.kura.metadata.MetadataManager
import com.kura.metadata.StatisticsInfo
import com.kura.query.Scan
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TablePlanTest {
    private lateinit var transaction: Transaction
    private lateinit var metadataManager: MetadataManager
    private lateinit var layout: Layout
    private lateinit var statisticsInfo: StatisticsInfo
    private lateinit var schema: Schema
    private lateinit var tablePlan: TablePlan
    private val tableName = "test_table"

    @BeforeEach
    fun setUp() {
        // Arrange
        transaction = mockk(relaxed = true)
        metadataManager = mockk(relaxed = true)
        layout = mockk(relaxed = true)
        statisticsInfo = mockk(relaxed = true)
        schema = mockk(relaxed = true)

        every { metadataManager.getLayout(tableName, transaction) } returns layout
        every { metadataManager.getStatisticsInfo(tableName, layout, transaction) } returns statisticsInfo
        every { layout.schema() } returns schema

        tablePlan = TablePlan(transaction, tableName, metadataManager)
    }

    @Test
    fun `blocksAccessed should return statistics info blocks accessed`() {
        // Arrange
        val expectedBlocks = 42
        every { statisticsInfo.blocksAccessed() } returns expectedBlocks

        // Act
        val result = tablePlan.blocksAccessed()

        // Assert
        assertEquals(expectedBlocks, result)
        verify { statisticsInfo.blocksAccessed() }
    }

    @Test
    fun `recordsOutput should return statistics info records output`() {
        // Arrange
        val expectedRecords = 100
        every { statisticsInfo.recordsOutput() } returns expectedRecords

        // Act
        val result = tablePlan.recordsOutput()

        // Assert
        assertEquals(expectedRecords, result)
        verify { statisticsInfo.recordsOutput() }
    }

    @Test
    fun `distinctValues should return statistics info distinct values`() {
        // Arrange
        val fieldName = "test_field"
        val expectedDistinctValues = 25
        every { statisticsInfo.distinctValues(fieldName) } returns expectedDistinctValues

        // Act
        val result = tablePlan.distinctValues(fieldName)

        // Assert
        assertEquals(expectedDistinctValues, result)
        verify { statisticsInfo.distinctValues(fieldName) }
    }

    @Test
    fun `schema should return layout schema`() {
        // Arrange
        // Already set up in setUp()

        // Act
        val result = tablePlan.schema()

        // Assert
        assertEquals(schema, result)
        verify { layout.schema() }
    }
}