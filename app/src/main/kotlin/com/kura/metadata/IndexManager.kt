package com.kura.metadata

import com.kura.record.*
import com.kura.transaction.Transaction
import java.util.*

/**
 * The index manager.
 * The index manager has similar functionality to the table manager.
 */
class IndexManager(
    isNew: Boolean,
    private val tableManager: TableManager,
    private val statisticsManager: StatisticsManager,
    tx: Transaction
) {
    private val layout: Layout

    init {
        if (isNew) {
            val schema = Schema()
            schema.addStringField("indexname", TableManager.MAX_NAME)
            schema.addStringField("tablename", TableManager.MAX_NAME)
            schema.addStringField("fieldname", TableManager.MAX_NAME)
            tableManager.createTable("idxcat", schema, tx)
        }
        layout = tableManager.getLayout("idxcat", tx)
    }

    /**
     * Create an index of the specified type for the specified field.
     * A unique ID is assigned to this index, and its information
     * is stored in the idxcat table.
     * @param indexName the name of the index
     * @param tableName the name of the indexed table
     * @param fieldName the name of the indexed field
     * @param tx the calling transaction
     */
    fun createIndex(
        indexName: String,
        tableName: String,
        fieldName: String,
        tx: Transaction
    ) {
        val tableScan = TableScan(tx, "idxcat", layout)
        tableScan.insert()
        tableScan.setString("indexname", indexName)
        tableScan.setString("tablename", tableName)
        tableScan.setString("fieldname", fieldName)
        tableScan.close()
    }

    /**
     * Return a map containing the index info for all indexes
     * on the specified table.
     * @param tableName the name of the table
     * @param tx the calling transaction
     * @return a map of IndexInfo objects, keyed by their field names
     */
    fun getIndexInfo(
        tableName: String,
        tx: Transaction
    ): Map<String, IndexInfo> {
        val result = HashMap<String, IndexInfo>()
        val tableScan = TableScan(tx, "idxcat", layout)

        while (tableScan.next()) {
            if (tableScan.getString("tablename") == tableName) {
                val indexName = tableScan.getString("indexname")
                val fieldName = tableScan.getString("fieldname")
                val tableLayout = tableManager.getLayout(tableName, tx)
                val tableStatistics = statisticsManager.getStatisticsInfo(tableName, tableLayout, tx)
                val indexInfo = IndexInfo(indexName, fieldName, tableLayout.schema(), tx, tableStatistics)
                result[fieldName] = indexInfo
            }
        }

        tableScan.close()
        return result
    }
}