package com.kura.metadata

import com.kura.record.*
import com.kura.transaction.Transaction

/**
 * Manages database views by allowing creation and retrieval of view definitions.
 */
class ViewManager(
    isNew: Boolean,
    private val tableManager: TableManager,
    tx: Transaction
) {
    companion object {
        // The max chars in a view definition.
        private const val MAX_VIEWDEF: Int = 100
    }

    init {
        if (isNew) {
            val schema = Schema()
            schema.addStringField("viewname", TableManager.MAX_NAME)
            schema.addStringField("viewdef", MAX_VIEWDEF)
            tableManager.createTable("viewcat", schema, tx)
        }
    }

    /**
     * Creates a new view with the given name and definition.
     *
     * @param viewName the name of the view
     * @param viewDefinition the SQL query that defines the view
     * @param tx the transaction
     */
    fun createView(viewName: String, viewDefinition: String, tx: Transaction) {
        val layout = tableManager.getLayout("viewcat", tx)
        val tableScan = TableScan(tx, "viewcat", layout)
        tableScan.insert()
        tableScan.setString("viewname", viewName)
        tableScan.setString("viewdef", viewDefinition)
        tableScan.close()
    }

    /**
     * Retrieves the definition of the specified view.
     *
     * @param viewName the name of the view
     * @param tx the transaction
     * @return the definition of the view, or null if not found
     */
    fun getViewDefinition(viewName: String, tx: Transaction): String? {
        var result: String? = null
        val layout = tableManager.getLayout("viewcat", tx)
        val tableScan = TableScan(tx, "viewcat", layout)
        while (tableScan.next()) {
            if (tableScan.getString("viewname") == viewName) {
                result = tableScan.getString("viewdef")
                break
            }
        }
        tableScan.close()
        return result
    }
}