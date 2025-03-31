package com.kura.jdbc.network

import java.rmi.Remote
import java.rmi.RemoteException

/**
 * The RMI remote interface corresponding to ResultSet.
 * The methods are identical to those of ResultSet,
 * except that they throw RemoteExceptions instead of SQLExceptions.
 */
interface RemoteResultSet : Remote {
    @Throws(RemoteException::class)
    fun next(): Boolean

    @Throws(RemoteException::class)
    fun getInt(fieldName: String): Int

    @Throws(RemoteException::class)
    fun getString(fieldName: String): String

    @Throws(RemoteException::class)
    fun getBoolean(fieldName: String): Boolean

    @Throws(RemoteException::class)
    fun close()

    @Throws(RemoteException::class)
    fun getMetaData(): RemoteMetaData
}