package com.kura.metadata

import com.kura.index.Index
import com.kura.index.hash.HashIndex
// import com.kura.index.btree.BTreeIndex // in case we change to btree indexing
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.transaction.Transaction
import java.sql.Types.INTEGER

/**
 * The information about an index.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the layout of the index records.
 * Its methods are essentially the same as those of Plan.
 */
class IndexInfo(
    private val indexName: String,
    private val fieldName: String,
    private val tableSchema: Schema,
    private val tx: Transaction,
    private val statisticsInfo: StatisticsInfo
) {
    private val indexLayout: Layout = createIndexLayout()

    /**
     * Open the index described by this object.
     * @return the Index object associated with this information
     */
    fun open(): Index {
        return HashIndex(tx, indexName, indexLayout)
        // return BTreeIndex(tx, indexName, indexLayout)
    }

    /**
     * Estimate the number of block accesses required to
     * find all index records having a particular search key.
     * The method uses the table's metadata to estimate the
     * size of the index file and the number of index records
     * per block.
     * It then passes this information to the traversalCost
     * method of the appropriate index type,
     * which provides the estimate.
     * @return the number of block accesses required to traverse the index
     */
    fun blocksAccessed(): Int {
        val recordsPerBlock = tx.blockSize() / indexLayout.slotSize()
        val numBlocks = statisticsInfo.recordsOutput() / recordsPerBlock
        return HashIndex.searchCost(numBlocks, recordsPerBlock)
        // return BTreeIndex.searchCost(numBlocks, recordsPerBlock)
    }

    /**
     * Return the estimated number of records having a
     * search key. This value is the same as doing a select
     * query; that is, it is the number of records in the table
     * divided by the number of distinct values of the indexed field.
     * @return the estimated number of records having a search key
     */
    fun recordsOutput(): Int {
        return statisticsInfo.recordsOutput() / statisticsInfo.distinctValues(fieldName)
    }

    /**
     * Return the distinct values for a specified field
     * in the underlying table, or 1 for the indexed field.
     * @param fieldName the specified field
     */
    fun distinctValues(fName: String): Int {
        return if (fieldName == fName) 1 else statisticsInfo.distinctValues(fieldName)
    }

    /**
     * Return the layout of the index records.
     * The schema consists of the dataRID (which is
     * represented as two integers, the block number and the
     * record ID) and the dataval (which is the indexed field).
     * Schema information about the indexed field is obtained
     * via the table's schema.
     * @return the layout of the index records
     */
    private fun createIndexLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")

        if (tableSchema.type(fieldName) == INTEGER) {
            schema.addIntField("dataval")
        } else {
            val fieldLength = tableSchema.length(fieldName)
            schema.addStringField("dataval", fieldLength)
        }

        return Layout(schema)
    }
}