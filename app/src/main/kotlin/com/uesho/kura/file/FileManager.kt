package com.uesho.kura.file

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

/**
 * The file manager handles the low-level operations of the database.
 * It is responsible for:
 * - Creating and deleting files
 * - Reading and writing blocks
 * - Appending new blocks to files
 *
 * @property dbDirectory The directory that contains the database files
 * @property blockSize The size of each block in bytes
 */
class FileManager(
    private val dbDirectory: File,
    private val blockSize: Int
) {
    private val isNew: Boolean = !dbDirectory.exists()
    private val openFiles: MutableMap<String, RandomAccessFile> = mutableMapOf()

    init {
        // Create the directory if the database is new
        if (isNew) {
            dbDirectory.mkdirs()
        }

        // Remove any leftover temporary tables
        dbDirectory.list()?.forEach { filename ->
            if (filename.startsWith("temp")) {
                File(dbDirectory, filename).delete()
            }
        }
    }

    /**
     * Read the contents of a block into a page.
     *
     * @param block The block to read
     * @param page The page to read into
     */
    @Synchronized
    fun read(block: BlockId, page: Page) {
        try {
            val file = getFile(block.fileName)
            file.seek(block.blockNum * blockSize.toLong())
            file.channel.read(page.contents())
        } catch (e: IOException) {
            throw RuntimeException("Cannot read block $block", e)
        }
    }

    /**
     * Write the contents of a page into a block.
     *
     * @param block The block to write to
     * @param page The page to write from
     */
    @Synchronized
    fun write(block: BlockId, page: Page) {
        try {
            val file = getFile(block.fileName)
            file.seek(block.blockNum * blockSize.toLong())
            file.channel.write(page.contents())
        } catch (e: IOException) {
            throw RuntimeException("Cannot write block $block", e)
        }
    }

    /**
     * Append a new block to the end of a file and return its block id.
     *
     * @param filename The name of the file
     * @return The block id of the new block
     */
    @Synchronized
    fun append(filename: String): BlockId {
        val newBlockNum = length(filename)
        val block = BlockId(filename, newBlockNum)
        val bytes = ByteArray(blockSize)

        try {
            val file = getFile(block.fileName)
            file.seek(block.blockNum * blockSize.toLong())
            file.write(bytes)
        } catch (e: IOException) {
            throw RuntimeException("Cannot append block $block", e)
        }

        return block
    }

    /**
     * Return the number of blocks in a file.
     *
     * @param filename The name of the file
     * @return The number of blocks
     */
    fun length(filename: String): Int {
        try {
            val file = getFile(filename)
            return (file.length() / blockSize).toInt()
        } catch (e: IOException) {
            throw RuntimeException("Cannot access $filename", e)
        }
    }

    /**
     * Return whether the database is new.
     *
     * @return True if the database is new, false otherwise
     */
    fun isNew(): Boolean = isNew

    /**
     * Return the size of each block in bytes.
     *
     * @return The block size
     */
    fun blockSize(): Int = blockSize

    /**
     * Get a RandomAccessFile for a filename, creating it if it doesn't exist.
     *
     * @param filename The name of the file
     * @return The RandomAccessFile
     */
    @Throws(IOException::class)
    private fun getFile(filename: String): RandomAccessFile {
        return openFiles.getOrPut(filename) {
            val dbTable = File(dbDirectory, filename)
            RandomAccessFile(dbTable, "rws")
        }
    }
}