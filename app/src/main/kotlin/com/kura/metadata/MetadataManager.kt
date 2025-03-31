package com.kura.metadata

import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.transaction.Transaction
import java.util.*

/**
 * The MetadataManager coordinates all the metadata managers.
 * It provides a unified interface for clients to access metadata services.
 */
class MetadataManager(isNew: Boolean, tx: Transaction) {
    private val tableManager: TableManager = TableManager(isNew, tx)
    private val viewManager: ViewManager = ViewManager(isNew, tableManager, tx)
    private val statisticsManager: StatisticsManager = StatisticsManager(tableManager, tx)
    private val indexManager: IndexManager = IndexManager(isNew, tableManager, statisticsManager, tx)

    /**
     * Creates a new table with the specified name and schema.
     *
     * @param tableName the name of the new table
     * @param schema the schema of the new table
     * @param tx the transaction creating the table
     */
    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        tableManager.createTable(tableName, schema, tx)
    }

    /**
     * Retrieves the layout of the specified table.
     *
     * @param tableName the name of the table
     * @param tx the transaction
     * @return the table's layout
     */
    fun getLayout(tableName: String, tx: Transaction): Layout {
        return tableManager.getLayout(tableName, tx)
    }

    /**
     * Creates a new view with the specified name and definition.
     *
     * @param viewName the name of the view
     * @param viewDefinition the SQL query that defines the view
     * @param tx the transaction creating the view
     */
    fun createView(viewName: String, viewDefinition: String, tx: Transaction) {
        viewManager.createView(viewName, viewDefinition, tx)
    }

    /**
     * Retrieves the definition of the specified view.
     *
     * @param viewName the name of the view
     * @param tx the transaction
     * @return the definition of the view
     */
    fun getViewDefinition(viewName: String, tx: Transaction): String? {
        return viewManager.getViewDefinition(viewName, tx)
    }

    /**
     * Creates a new index on the specified field of the specified table.
     *
     * @param indexName the name of the index
     * @param tableName the name of the indexed table
     * @param fieldName the name of the indexed field
     * @param tx the transaction creating the index
     */
    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        indexManager.createIndex(indexName, tableName, fieldName, tx)
    }

    /**
     * Retrieves the IndexInfo objects for all indexes on the specified table.
     *
     * @param tableName the name of the table
     * @param tx the transaction
     * @return a map of IndexInfo objects, keyed by their field names
     */
    fun getIndexInfo(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        return indexManager.getIndexInfo(tableName, tx)
    }

    /**
     * Retrieves the statistical information about the specified table.
     *
     * @param tableName the name of the table
     * @param layout the table's layout
     * @param tx the transaction
     * @return the statistical information about the table
     */
    fun getStatisticsInfo(tableName: String, layout: Layout, tx: Transaction): StatisticsInfo {
        return statisticsManager.getStatisticsInfo(tableName, layout, tx)
    }
}