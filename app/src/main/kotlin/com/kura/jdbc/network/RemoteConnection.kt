package com.kura.jdbc.network

import java.rmi.Remote
import java.rmi.RemoteException

/**
 * The RMI remote interface corresponding to Connection.
 * The methods are identical to those of Connection,
 * except that they throw RemoteExceptions instead of SQLExceptions.
 */
interface RemoteConnection : Remote {
    @Throws(RemoteException::class)
    fun createStatement(): RemoteStatement

    @Throws(RemoteException::class)
    fun close()
}