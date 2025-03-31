package com.kura.server

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.metadata.MetadataManager
import com.kura.transaction.Transaction
import java.io.File

/**
 * The class that configures the database system.
 */
class KuraDB {
    companion object {
        const val BLOCK_SIZE: Int = 400
        const val BUFFER_SIZE: Int = 8
        const val LOG_FILE: String = "kura.log"
    }

    private val fileManager: FileManager
    private val bufferManager: BufferManager
    private val logManager: LogManager
    private var metadataManager: MetadataManager? = null

    /**
     * A constructor useful for debugging.
     * @param dirname the name of the database directory
     * @param blockSize the block size
     * @param bufferSize the number of buffers
     */
    constructor(dirname: String, blockSize: Int, bufferSize: Int) {
        val dbDirectory = File(dirname)
        fileManager = FileManager(dbDirectory, blockSize)
        logManager = LogManager(fileManager, LOG_FILE)
        bufferManager = BufferManager(fileManager, logManager, bufferSize)
    }

    /**
     * A simpler constructor for most situations. Unlike the
     * 3-arg constructor, it also initializes the metadata tables.
     * @param dirname the name of the database directory
     */
    constructor(dirname: String) : this(dirname, BLOCK_SIZE, BUFFER_SIZE) {
        val transaction = newTransaction()
        val isNew = fileManager.isNew()
        if (isNew) {
            println("Creating new database")
        } else {
            println("Recovering existing database")
            transaction.recover()
        }
        metadataManager = MetadataManager(isNew, transaction)
        transaction.commit()
    }

    /**
     * A convenient way for clients to create transactions
     * and access the metadata.
     */
    fun newTransaction(): Transaction {
        return Transaction(fileManager, logManager, bufferManager)
    }

    fun metadataManager(): MetadataManager? {
        return metadataManager
    }

    // These methods aid in debugging
    fun fileManager(): FileManager {
        return fileManager
    }

    fun logManager(): LogManager {
        return logManager
    }

    fun bufferManager(): BufferManager {
        return bufferManager
    }
}