package com.kura.jdbc.network

import com.kura.jdbc.ResultSetMetaDataAdapter
import java.sql.SQLException
import java.sql.Types

/**
 * An implementation of ResultSetMetaData that connects to the remote metadata.
 */
class NetworkMetaData(private val rmd: RemoteMetaData) : ResultSetMetaDataAdapter() {

    override fun getColumnCount(): Int {
        try {
            return rmd.getColumnCount()
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getColumnName(column: Int): String {
        try {
            return rmd.getColumnName(column)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getColumnType(column: Int): Int {
        try {
            return rmd.getColumnType(column)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }

    override fun getColumnDisplaySize(column: Int): Int {
        try {
            return rmd.getColumnDisplaySize(column)
        } catch (e: Exception) {
            throw SQLException(e)
        }
    }
}