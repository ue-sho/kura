package com.kura.jdbc.network

import com.kura.plan.Planner
import com.kura.server.KuraDB
import com.kura.transaction.Transaction
import java.rmi.RemoteException
import java.rmi.server.UnicastRemoteObject

/**
 * The RMI server-side implementation of RemoteConnection.
 */
class RemoteConnectionImpl : UnicastRemoteObject, RemoteConnection {
    private val db: KuraDB
    private var currentTransaction: Transaction
    private var planner: Planner

    /**
     * Creates a remote connection
     * and begins a new transaction for it.
     */
    constructor(db: KuraDB) : super() {
        this.db = db
        this.currentTransaction = db.newTransaction()
        this.planner = db.planner()!!
    }

    /**
     * Creates a new RemoteStatement for this connection.
     */
    @Throws(RemoteException::class)
    override fun createStatement(): RemoteStatement {
        return RemoteStatementImpl(this, planner)
    }

    /**
     * Closes the connection.
     * The current transaction is committed.
     */
    @Throws(RemoteException::class)
    override fun close() {
        currentTransaction.commit()
    }

    // The following methods are used by the server-side classes.

    /**
     * Returns the transaction currently associated with
     * this connection.
     * @return the transaction associated with this connection
     */
    fun getTransaction(): Transaction {
        return currentTransaction
    }

    /**
     * Commits the current transaction,
     * and begins a new one.
     */
    fun commit() {
        currentTransaction.commit()
        currentTransaction = db.newTransaction()
    }

    /**
     * Rolls back the current transaction,
     * and begins a new one.
     */
    fun rollback() {
        currentTransaction.rollback()
        currentTransaction = db.newTransaction()
    }
}