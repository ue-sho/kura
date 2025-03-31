package com.kura.plan

import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.CreateViewData
import com.kura.parse.DeleteData
import com.kura.parse.InsertData
import com.kura.parse.ModifyData
import com.kura.transaction.Transaction

/**
 * The interface implemented by the planners
 * for SQL insert, delete, and modify statements.
 */
interface UpdatePlanner {
    /**
     * Executes the specified insert statement, and
     * returns the number of affected records.
     * @param data the parsed representation of the insert statement
     * @param transaction the calling transaction
     * @return the number of affected records
     */
    fun executeInsert(data: InsertData, transaction: Transaction): Int

    /**
     * Executes the specified delete statement, and
     * returns the number of affected records.
     * @param data the parsed representation of the delete statement
     * @param transaction the calling transaction
     * @return the number of affected records
     */
    fun executeDelete(data: DeleteData, transaction: Transaction): Int

    /**
     * Executes the specified modify statement, and
     * returns the number of affected records.
     * @param data the parsed representation of the modify statement
     * @param transaction the calling transaction
     * @return the number of affected records
     */
    fun executeModify(data: ModifyData, transaction: Transaction): Int

    /**
     * Executes the specified create table statement, and
     * returns the number of affected records.
     * @param data the parsed representation of the create table statement
     * @param transaction the calling transaction
     * @return the number of affected records
     */
    fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int

    /**
     * Executes the specified create view statement, and
     * returns the number of affected records.
     * @param data the parsed representation of the create view statement
     * @param transaction the calling transaction
     * @return the number of affected records
     */
    fun executeCreateView(data: CreateViewData, transaction: Transaction): Int

    /**
     * Executes the specified create index statement, and
     * returns the number of affected records.
     * @param data the parsed representation of the create index statement
     * @param transaction the calling transaction
     * @return the number of affected records
     */
    fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int
}