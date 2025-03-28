package com.kura.file

/**
 * A block identifier that uniquely identifies a block in the database.
 * A block is identified by its file name and block number within that file.
 *
 * @property fileName The name of the file containing the block
 * @property blockNum The number of the block within the file
 */
data class BlockId(
    val fileName: String,
    val blockNum: Int
) {
    override fun toString(): String = "[file $fileName, block $blockNum]"
}