package com.kura.index.btree

import com.kura.file.BlockId
import com.kura.index.Index
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.record.Schema
import com.kura.transaction.Transaction
import java.sql.Types
import kotlin.math.ln

/**
 * A B-tree implementation of the Index interface.
 */
class BTreeIndex(
    private val tx: Transaction,
    private val indexName: String,
    private val leafLayout: Layout
) : Index {
    companion object {
        /**
         * Estimates the number of block accesses required to find
         * all index records having a particular search key.
         */
        @JvmStatic
        fun searchCost(numBlocks: Int, recordsPerBlock: Int): Int {
            return 1 + (ln(numBlocks.toDouble()) / ln(recordsPerBlock.toDouble())).toInt()
        }
    }

    private val dirLayout: Layout
    private val leafTable: String = "${indexName}leaf"
    private val dirTable: String = "${indexName}dir"
    private var leaf: BTreeLeaf? = null
    private var rootBlock: BlockId

    init {
        // Open (or create) the leaf file.
        if (tx.size(leafTable) == 0) {
            val block = tx.append(leafTable)
            val node = BTPage(tx, block, leafLayout)
            node.format(block, -1)
            node.close()
        }

        // Build the directory schema. It's a copy of the leaf schema
        // but without the "id" field.
        val dirSchema = Schema()
        dirSchema.add("block", leafLayout.schema())
        dirSchema.add("dataval", leafLayout.schema())
        dirLayout = Layout(dirSchema)

        rootBlock = BlockId(dirTable, 0)

        // Open (or create) the directory file.
        if (tx.size(dirTable) == 0) {
            // Create a new root block.
            tx.append(dirTable)
            val node = BTPage(tx, rootBlock, dirLayout)
            node.format(rootBlock, 0)
            // Insert initial entry: the minval with a pointer to block 0 of the leaf file.
            val minVal = if (dirSchema.type("dataval") == Types.INTEGER) {
                Constant(Int.MIN_VALUE)
            } else {
                Constant("")
            }
            node.insertDir(0, minVal, 0)
            node.close()
        }
    }

    override fun beforeFirst(searchKey: Any) {
        close()
        val key = searchKey as Constant
        val root = BTreeDir(tx, dirLayout, rootBlock)
        val blockNum = root.search(key)
        root.close()
        val leafBlock = BlockId(leafTable, blockNum)
        leaf = BTreeLeaf(tx, leafLayout, key, leafBlock)
    }

    override fun next(): Boolean {
        return leaf!!.next()
    }

    override fun getDataRecordId(): RecordId {
        return leaf!!.getDataRecordId()
    }

    override fun insert(key: Any, recordId: RecordId) {
        beforeFirst(key)
        val dataVal = key as Constant
        val entry = leaf!!.insert(dataVal, recordId)
        leaf!!.close()
        if (entry == null) {
            return
        }
        val root = BTreeDir(tx, dirLayout, rootBlock)
        val entry2 = root.insert(entry)
        if (entry2 != null) {
            root.makeNewRoot(entry2)
        }
        root.close()
    }

    override fun delete(key: Any, recordId: RecordId) {
        beforeFirst(key)
        val dataVal = key as Constant
        leaf!!.delete(dataVal, recordId)
        leaf!!.close()
    }

    override fun close() {
        leaf?.close()
        leaf = null
    }
}
