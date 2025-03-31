package com.kura.metadata

import com.kura.record.*
import com.kura.transaction.Transaction
import java.util.*

/**
 * The statistics manager is responsible for
 * keeping statistical information about each table.
 * The manager does not store this information in the database.
 * Instead, it calculates this information on system startup,
 * and periodically refreshes it.
 */
class StatisticsManager(
    private val tableManager: TableManager,
    tx: Transaction
) {
    private var tableStatistics: MutableMap<String, StatisticsInfo> = HashMap()
    private var numberOfCalls: Int = 0

    init {
        refreshStatistics(tx)
    }

    /**
     * Return the statistical information about the specified table.
     * @param tableName the name of the table
     * @param layout the table's layout
     * @param tx the calling transaction
     * @return the statistical information about the table
     */
    @Synchronized
    fun getStatisticsInfo(
        tableName: String,
        layout: Layout,
        tx: Transaction
    ): StatisticsInfo {
        numberOfCalls++
        if (numberOfCalls > 100) {
            refreshStatistics(tx)
        }

        return tableStatistics[tableName] ?: run {
            val statisticsInfo = calculateTableStatistics(tableName, layout, tx)
            tableStatistics[tableName] = statisticsInfo
            statisticsInfo
        }
    }

    /**
     * Refreshes the statistics for all tables in the database.
     * This is called on system startup and periodically when needed.
     */
    @Synchronized
    private fun refreshStatistics(tx: Transaction) {
        tableStatistics = HashMap()
        numberOfCalls = 0

        val tableCatalogLayout = tableManager.getLayout("tblcat", tx)
        val tableCatalog = TableScan(tx, "tblcat", tableCatalogLayout)

        while (tableCatalog.next()) {
            val tableName = tableCatalog.getString("tblname")
            val layout = tableManager.getLayout(tableName, tx)
            val statisticsInfo = calculateTableStatistics(tableName, layout, tx)
            tableStatistics[tableName] = statisticsInfo
        }

        tableCatalog.close()
    }

    /**
     * Calculates the statistics for a specific table by scanning through all records.
     */
    @Synchronized
    private fun calculateTableStatistics(
        tableName: String,
        layout: Layout,
        tx: Transaction
    ): StatisticsInfo {
        var recordCount = 0
        var blockCount = 0

        val tableScan = TableScan(tx, tableName, layout)
        while (tableScan.next()) {
            recordCount++
            blockCount = tableScan.getRecordId().blockNumber() + 1
        }
        tableScan.close()

        return StatisticsInfo(blockCount, recordCount)
    }
}