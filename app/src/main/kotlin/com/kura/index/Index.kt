package com.kura.index

import com.kura.record.RecordId

/**
 * An interface that defines the basic operations of any index.
 */
interface Index {
    /**
     * Positions the index before the first record having the specified search key.
     * @param searchKey the search key value
     */
    fun beforeFirst(searchKey: Any)

    /**
     * Moves the index to the next record having the search key.
     * @return false if there is no next record with the search key
     */
    fun next(): Boolean

    /**
     * Returns the data record ID value stored in the current index record.
     * @return the data record ID
     */
    fun getDataRecordId(): RecordId

    /**
     * Inserts an index record having the specified key and data record ID.
     * @param key the key in the index
     * @param recordId the data record ID
     */
    fun insert(key: Any, recordId: RecordId)

    /**
     * Deletes the index record having the specified key and data record ID.
     * @param key the key in the index
     * @param recordId the data record ID
     */
    fun delete(key: Any, recordId: RecordId)

    /**
     * Closes the index.
     */
    fun close()
}