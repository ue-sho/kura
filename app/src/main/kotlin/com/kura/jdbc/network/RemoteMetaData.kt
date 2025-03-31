package com.kura.jdbc.network

import java.rmi.Remote
import java.rmi.RemoteException

/**
 * The RMI remote interface corresponding to ResultSetMetaData.
 * The methods are identical to those of ResultSetMetaData,
 * except that they throw RemoteExceptions instead of SQLExceptions.
 */
interface RemoteMetaData : Remote {
    @Throws(RemoteException::class)
    fun getColumnCount(): Int

    @Throws(RemoteException::class)
    fun getColumnName(column: Int): String

    @Throws(RemoteException::class)
    fun getColumnType(column: Int): Int

    @Throws(RemoteException::class)
    fun getColumnDisplaySize(column: Int): Int
}
