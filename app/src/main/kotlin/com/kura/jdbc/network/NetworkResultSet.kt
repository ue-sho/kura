package com.kura.jdbc.network

import com.kura.jdbc.ResultSetAdapter
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException

/**
 * An adapter class that wraps RemoteResultSet.
 * Its methods transform RemoteExceptions into SQLExceptions.
 */
class NetworkResultSet(private val remoteResultSet: RemoteResultSet) : ResultSetAdapter() {
    override fun next(): Boolean {
        return try {
            remoteResultSet.next()
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getInt(columnLabel: String): Int {
        return try {
            remoteResultSet.getInt(columnLabel)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getString(columnLabel: String): String {
        return try {
            remoteResultSet.getString(columnLabel)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getBoolean(columnLabel: String): Boolean {
        return try {
            remoteResultSet.getBoolean(columnLabel)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun close() {
        try {
            remoteResultSet.close()
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getMetaData(): ResultSetMetaData {
        try {
            val rmd: RemoteMetaData = remoteResultSet.getMetaData()
            return NetworkMetaData(rmd)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getObject(columnLabel: String): Any {
        // Try to convert to the closest type
        try {
            val intValue = remoteResultSet.getInt(columnLabel)
            return intValue
        } catch (e: Exception) {
            // If not an integer, get as string
            try {
                return remoteResultSet.getString(columnLabel)
            } catch (e2: Exception) {
                throw SQLException(e2)
            }
        }
    }

    override fun getConcurrency(): Int {
        return ResultSet.CONCUR_READ_ONLY
    }

    override fun getType(): Int {
        return ResultSet.TYPE_FORWARD_ONLY
    }

    override fun getHoldability(): Int {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT
    }

    override fun isClosed(): Boolean {
        return false
    }
}