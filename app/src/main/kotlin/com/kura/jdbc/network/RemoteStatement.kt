package com.kura.jdbc.network

import java.rmi.Remote
import java.rmi.RemoteException

/**
 * The RMI remote interface corresponding to Statement.
 * The methods are identical to those of Statement,
 * except that they throw RemoteExceptions instead of SQLExceptions.
 */
interface RemoteStatement : Remote {
    @Throws(RemoteException::class)
    fun executeQuery(query: String): RemoteResultSet

    @Throws(RemoteException::class)
    fun executeUpdate(command: String): Int

    @Throws(RemoteException::class)
    fun close()
}