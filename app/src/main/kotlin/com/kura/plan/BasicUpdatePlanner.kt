package com.kura.plan

import com.kura.metadata.MetadataManager
import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.CreateViewData
import com.kura.parse.DeleteData
import com.kura.parse.InsertData
import com.kura.parse.ModifyData
import com.kura.query.UpdateScan
import com.kura.transaction.Transaction

/**
 * The basic planner for SQL update statements.
 */
class BasicUpdatePlanner(private val metadataManager: MetadataManager) : UpdatePlanner {

    override fun executeDelete(data: DeleteData, transaction: Transaction): Int {
        var plan: Plan = TablePlan(transaction, data.tableName(), metadataManager)
        plan = SelectPlan(plan, data.pred())
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            updateScan.delete()
            count++
        }
        updateScan.close()
        return count
    }

    override fun executeModify(data: ModifyData, transaction: Transaction): Int {
        var plan: Plan = TablePlan(transaction, data.tableName(), metadataManager)
        plan = SelectPlan(plan, data.pred())
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            val value = data.newValue().evaluate(updateScan)
            updateScan.setVal(data.fieldName(), value)
            count++
        }
        updateScan.close()
        return count
    }

    override fun executeInsert(data: InsertData, transaction: Transaction): Int {
        val plan = TablePlan(transaction, data.tableName(), metadataManager)
        val updateScan = plan.open() as UpdateScan
        updateScan.insert()
        val iter = data.values().iterator()
        for (fieldName in data.fields()) {
            val value = iter.next()
            updateScan.setVal(fieldName, value)
        }
        updateScan.close()
        return 1
    }

    override fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int {
        metadataManager.createTable(data.tableName(), data.schema(), transaction)
        return 0
    }

    override fun executeCreateView(data: CreateViewData, transaction: Transaction): Int {
        metadataManager.createView(data.viewName(), data.viewDef().toString(), transaction)
        return 0
    }

    override fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int {
        metadataManager.createIndex(data.indexName(), data.tableName(), data.fieldName(), transaction)
        return 0
    }
}