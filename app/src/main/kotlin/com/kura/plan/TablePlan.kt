package com.kura.plan

import com.kura.metadata.MetadataManager
import com.kura.metadata.StatisticsInfo
import com.kura.query.Scan
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction

/**
 * The Plan class corresponding to a table.
 */
class TablePlan(
    private val transaction: Transaction,
    private val tableName: String,
    metadataManager: MetadataManager
) : Plan {
    private val layout: Layout
    private val statisticsInfo: StatisticsInfo

    init {
        layout = metadataManager.getLayout(tableName, transaction)
        statisticsInfo = metadataManager.getStatisticsInfo(tableName, layout, transaction)
    }

    /**
     * Creates a table scan for this query.
     * @see Plan.open
     */
    override fun open(): Scan {
        return TableScan(transaction, tableName, layout)
    }

    /**
     * Estimates the number of block accesses for the table,
     * which is obtainable from the statistics manager.
     * @see Plan.blocksAccessed
     */
    override fun blocksAccessed(): Int {
        return statisticsInfo.blocksAccessed()
    }

    /**
     * Estimates the number of records in the table,
     * which is obtainable from the statistics manager.
     * @see Plan.recordsOutput
     */
    override fun recordsOutput(): Int {
        return statisticsInfo.recordsOutput()
    }

    /**
     * Estimates the number of distinct field values in the table,
     * which is obtainable from the statistics manager.
     * @see Plan.distinctValues
     */
    override fun distinctValues(fieldName: String): Int {
        return statisticsInfo.distinctValues(fieldName)
    }

    /**
     * Determines the schema of the table,
     * which is obtainable from the catalog manager.
     * @see Plan.schema
     */
    override fun schema(): Schema {
        return layout.schema()
    }
}