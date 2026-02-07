package com.kura.index.planner

import com.kura.index.Index
import com.kura.metadata.MetadataManager
import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.CreateViewData
import com.kura.parse.DeleteData
import com.kura.parse.InsertData
import com.kura.parse.ModifyData
import com.kura.plan.Plan
import com.kura.plan.SelectPlan
import com.kura.plan.TablePlan
import com.kura.plan.UpdatePlanner
import com.kura.query.UpdateScan
import com.kura.transaction.Transaction

/**
 * A modification of the basic update planner.
 * It dispatches each update statement to the corresponding
 * index planner, maintaining indexes on insert, delete, and modify.
 */
class IndexUpdatePlanner(private val metadataManager: MetadataManager) : UpdatePlanner {

    override fun executeInsert(data: InsertData, transaction: Transaction): Int {
        val tableName = data.tableName()
        val plan = TablePlan(transaction, tableName, metadataManager)

        // First, insert the record
        val scan = plan.open() as UpdateScan
        scan.insert()
        val rid = scan.getRecordId()

        // Then modify each field, inserting an index record if appropriate
        val indexes = metadataManager.getIndexInfo(tableName, transaction)
        val valIter = data.values().iterator()
        for (fieldName in data.fields()) {
            val value = valIter.next()
            scan.setVal(fieldName, value)

            val indexInfo = indexes[fieldName]
            if (indexInfo != null) {
                val idx: Index = indexInfo.open()
                idx.insert(value, rid)
                idx.close()
            }
        }
        scan.close()
        return 1
    }

    override fun executeDelete(data: DeleteData, transaction: Transaction): Int {
        val tableName = data.tableName()
        var plan: Plan = TablePlan(transaction, tableName, metadataManager)
        plan = SelectPlan(plan, data.pred())
        val indexes = metadataManager.getIndexInfo(tableName, transaction)

        val scan = plan.open() as UpdateScan
        var count = 0
        while (scan.next()) {
            // First, delete the record's RID from every index
            val rid = scan.getRecordId()
            for (fieldName in indexes.keys) {
                val value = scan.getVal(fieldName)
                val idx: Index = indexes[fieldName]!!.open()
                idx.delete(value, rid)
                idx.close()
            }
            // Then delete the record
            scan.delete()
            count++
        }
        scan.close()
        return count
    }

    override fun executeModify(data: ModifyData, transaction: Transaction): Int {
        val tableName = data.tableName()
        val fieldName = data.fieldName()
        var plan: Plan = TablePlan(transaction, tableName, metadataManager)
        plan = SelectPlan(plan, data.pred())

        val indexInfo = metadataManager.getIndexInfo(tableName, transaction)[fieldName]
        val idx: Index? = indexInfo?.open()

        val scan = plan.open() as UpdateScan
        var count = 0
        while (scan.next()) {
            // First, update the record
            val newVal = data.newValue().evaluate(scan)
            val oldVal = scan.getVal(fieldName)
            scan.setVal(data.fieldName(), newVal)

            // Then update the appropriate index, if it exists
            if (idx != null) {
                val rid = scan.getRecordId()
                idx.delete(oldVal, rid)
                idx.insert(newVal, rid)
            }
            count++
        }
        idx?.close()
        scan.close()
        return count
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
