package com.uesho.kura.log

import com.uesho.kura.file.BlockId
import com.uesho.kura.file.FileManager
import com.uesho.kura.file.Page

/**
 * The log manager, which is responsible for writing log records into a log file.
 * The tail of the log is kept in a byte buffer, which is flushed to disk when needed.
 */
class LogManager(
    private val fileManager: FileManager,
    private val logFile: String
) {
    private val logPage: Page
    private var currentBlock: BlockId
    private var latestLSN: Int = 0
    private var lastSavedLSN: Int = 0

    init {
        val bytes = ByteArray(fileManager.blockSize())
        logPage = Page(bytes)
        val logSize = fileManager.length(logFile)

        currentBlock = if (logSize == 0) {
            appendNewBlock()
        } else {
            BlockId(logFile, logSize - 1).also { block ->
                fileManager.read(block, logPage)
            }
        }
    }

    /**
     * Ensures that the log record corresponding to the specified LSN has been written to disk.
     * All earlier log records will also be written to disk.
     *
     * @param lsn the LSN of a log record
     */
    fun flush(lsn: Int) {
        if (lsn >= lastSavedLSN) {
            flush()
        }
    }

    /**
     * Appends a log record to the log buffer.
     * The record consists of an arbitrary array of bytes.
     * Log records are written right to left in the buffer.
     * The size of the record is written before the bytes.
     * The beginning of the buffer contains the location of the last-written record (the "boundary").
     * Storing the records backwards makes it easy to read them in reverse order.
     *
     * @param logRecord a byte buffer containing the bytes
     * @return the LSN of the final value
     */
    @Synchronized
    fun append(logRecord: ByteArray): Int {
        val boundary = logPage.getInt(0)
        val recordSize = logRecord.size
        val bytesNeeded = recordSize + Int.SIZE_BYTES

        if (boundary - bytesNeeded < Int.SIZE_BYTES) {
            flush()
            currentBlock = appendNewBlock()
            val newBoundary = logPage.getInt(0)
            val recordPos = newBoundary - bytesNeeded
            logPage.setBytes(recordPos, logRecord)
            logPage.setInt(0, recordPos)
        } else {
            val recordPos = boundary - bytesNeeded
            logPage.setBytes(recordPos, logRecord)
            logPage.setInt(0, recordPos)
        }

        latestLSN += 1
        return latestLSN
    }

    fun iterator(): Iterator<ByteArray> {
        flush()
        return LogIterator(fileManager, currentBlock)
    }

    /**
     * Initialize the byte buffer and append it to the log file.
     */
    private fun appendNewBlock(): BlockId {
        return fileManager.append(logFile).also { block ->
            logPage.setInt(0, fileManager.blockSize())
            fileManager.write(block, logPage)
        }
    }

    /**
     * Write the buffer to the log file.
     */
    private fun flush() {
        fileManager.write(currentBlock, logPage)
        lastSavedLSN = latestLSN
    }
}