package com.kura.materialize

import com.kura.query.UpdateScan
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction

/**
 * A class that creates temporary tables.
 * A temporary table is not registered in the catalog.
 * The class therefore has a method getLayout to return the
 * table's metadata.
 */
class TempTable(
    private val transaction: Transaction,
    schema: Schema
) {
    private val tableName: String = nextTableName()
    private val layout: Layout = Layout(schema)

    /**
     * Open a table scan for the temporary table.
     */
    fun open(): UpdateScan {
        return TableScan(transaction, tableName, layout)
    }

    /**
     * Return the table's name.
     */
    fun tableName(): String {
        return tableName
    }

    /**
     * Return the table's metadata.
     */
    fun getLayout(): Layout {
        return layout
    }

    companion object {
        private var nextTableNum = 0

        @Synchronized
        private fun nextTableName(): String {
            nextTableNum++
            return "temp$nextTableNum"
        }
    }
}
