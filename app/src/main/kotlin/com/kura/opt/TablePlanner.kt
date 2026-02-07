package com.kura.opt

import com.kura.index.planner.IndexJoinPlan
import com.kura.index.planner.IndexSelectPlan
import com.kura.metadata.IndexInfo
import com.kura.metadata.MetadataManager
import com.kura.multibuffer.MultibufferProductPlan
import com.kura.plan.Plan
import com.kura.plan.SelectPlan
import com.kura.plan.TablePlan
import com.kura.query.Predicate
import com.kura.record.Schema
import com.kura.transaction.Transaction

/**
 * This class contains methods for planning a single table.
 */
class TablePlanner(
    tableName: String,
    private val predicate: Predicate,
    private val transaction: Transaction,
    metadataManager: MetadataManager
) {
    private val myPlan: TablePlan = TablePlan(transaction, tableName, metadataManager)
    private val mySchema: Schema = myPlan.schema()
    private val indexes: Map<String, IndexInfo> = metadataManager.getIndexInfo(tableName, transaction)

    /**
     * Constructs a select plan for the table.
     * The plan will use an indexselect, if possible.
     * @return a select plan for the table.
     */
    fun makeSelectPlan(): Plan {
        var p: Plan = makeIndexSelect() ?: myPlan
        return addSelectPredicate(p)
    }

    /**
     * Constructs a join plan of the specified plan
     * and the table. The plan will use an indexjoin, if possible.
     * (Which means that if an indexselect is also possible,
     * the indexjoin operator takes precedence.)
     * The method returns null if no join is possible.
     * @param current the specified plan
     * @return a join plan of the plan and this table
     */
    fun makeJoinPlan(current: Plan): Plan? {
        val currentSchema = current.schema()
        val joinPredicate = predicate.joinSubPredicate(mySchema, currentSchema) ?: return null
        val p = makeIndexJoin(current, currentSchema) ?: makeProductJoin(current, currentSchema)
        return p
    }

    /**
     * Constructs a product plan of the specified plan and
     * this table.
     * @param current the specified plan
     * @return a product plan of the specified plan and this table
     */
    fun makeProductPlan(current: Plan): Plan {
        val p = addSelectPredicate(myPlan)
        return MultibufferProductPlan(transaction, current, p)
    }

    private fun makeIndexSelect(): Plan? {
        for ((fieldName, indexInfo) in indexes) {
            val value = predicate.equatesWithConstant(fieldName)
            if (value != null) {
                return IndexSelectPlan(myPlan, indexInfo, value)
            }
        }
        return null
    }

    private fun makeIndexJoin(current: Plan, currentSchema: Schema): Plan? {
        for ((fieldName, indexInfo) in indexes) {
            val outerField = predicate.equatesWithField(fieldName)
            if (outerField != null && currentSchema.hasField(outerField)) {
                var p: Plan = IndexJoinPlan(current, myPlan, indexInfo, outerField)
                p = addSelectPredicate(p)
                return addJoinPredicate(p, currentSchema)
            }
        }
        return null
    }

    private fun makeProductJoin(current: Plan, currentSchema: Schema): Plan {
        val p = makeProductPlan(current)
        return addJoinPredicate(p, currentSchema)
    }

    private fun addSelectPredicate(plan: Plan): Plan {
        val selectPredicate = predicate.selectSubPredicate(mySchema)
        return if (selectPredicate != null) {
            SelectPlan(plan, selectPredicate)
        } else {
            plan
        }
    }

    private fun addJoinPredicate(plan: Plan, currentSchema: Schema): Plan {
        val joinPredicate = predicate.joinSubPredicate(currentSchema, mySchema)
        return if (joinPredicate != null) {
            SelectPlan(plan, joinPredicate)
        } else {
            plan
        }
    }
}
