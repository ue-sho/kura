package com.kura.index.btree

import com.kura.query.Constant

/**
 * A directory entry has two components: the block number of the child block,
 * and the dataval of the first record in that child block.
 */
data class DirEntry(val dataVal: Constant, val blockNumber: Int)
