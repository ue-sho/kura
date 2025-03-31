package com.kura.jdbc.network

import com.kura.jdbc.StatementAdapter
import java.sql.ResultSet
import java.sql.SQLException

/**
 * An adapter class that wraps RemoteStatement.
 * Its methods transform RemoteExceptions into SQLExceptions.
 */
class NetworkStatement(private val remoteStatement: RemoteStatement) : StatementAdapter() {
    override fun executeQuery(sql: String): ResultSet {
        return try {
            val remoteRs = remoteStatement.executeQuery(sql)
            NetworkResultSet(remoteRs)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun executeUpdate(sql: String): Int {
        return try {
            remoteStatement.executeUpdate(sql)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun close() {
        try {
            remoteStatement.close()
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getUpdateCount(): Int {
        return -1
    }

    override fun getResultSetType(): Int {
        return ResultSet.TYPE_FORWARD_ONLY
    }

    override fun getResultSetConcurrency(): Int {
        return ResultSet.CONCUR_READ_ONLY
    }

    override fun getResultSetHoldability(): Int {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT
    }

    override fun isClosed(): Boolean {
        return false
    }

    override fun isPoolable(): Boolean {
        return false
    }

    override fun isCloseOnCompletion(): Boolean {
        return false
    }
}