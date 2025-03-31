package com.kura.jdbc.network

import com.kura.jdbc.ConnectionAdapter
import java.sql.SQLException
import java.sql.Statement

/**
 * An adapter class that wraps RemoteConnection.
 * Its methods transform RemoteExceptions into SQLExceptions.
 */
class NetworkConnection(private val remoteConnection: RemoteConnection) : ConnectionAdapter() {
    override fun createStatement(): Statement {
        return try {
            val remoteStatement = remoteConnection.createStatement()
            NetworkStatement(remoteStatement)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun close() {
        try {
            remoteConnection.close()
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }
}