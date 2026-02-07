package com.kura.materialize

import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.query.UpdateScan
import com.kura.record.Schema
import com.kura.transaction.Transaction

/**
 * The Plan class for the sort operator.
 */
class SortPlan(
    private val transaction: Transaction,
    private val plan: Plan,
    sortFields: List<String>
) : Plan {
    private val schema: Schema = plan.schema()
    private val comp: RecordComparator = RecordComparator(sortFields)

    /**
     * This method is where most of the action is.
     * Up to 2 sorted temporary tables are created,
     * and are passed into SortScan for final merging.
     */
    override fun open(): Scan {
        val src = plan.open()
        var runs = splitIntoRuns(src)
        src.close()
        while (runs.size > 2) {
            runs = doAMergeIteration(runs)
        }
        if (runs.isEmpty()) {
            runs.add(TempTable(transaction, schema))
        }
        return SortScan(runs, comp)
    }

    /**
     * Return the number of blocks in the sorted table,
     * which is the same as it would be in a
     * materialized table.
     * It does not include the one-time cost
     * of materializing and sorting the records.
     */
    override fun blocksAccessed(): Int {
        val mp = MaterializePlan(transaction, plan)
        return mp.blocksAccessed()
    }

    /**
     * Return the number of records in the sorted table,
     * which is the same as in the underlying query.
     */
    override fun recordsOutput(): Int {
        return plan.recordsOutput()
    }

    /**
     * Return the number of distinct field values in
     * the sorted table, which is the same as in
     * the underlying query.
     */
    override fun distinctValues(fieldName: String): Int {
        return plan.distinctValues(fieldName)
    }

    /**
     * Return the schema of the sorted table, which
     * is the same as in the underlying query.
     */
    override fun schema(): Schema {
        return schema
    }

    private fun splitIntoRuns(src: Scan): MutableList<TempTable> {
        val temps = mutableListOf<TempTable>()
        src.beforeFirst()
        if (!src.next()) {
            return temps
        }
        var currentTemp = TempTable(transaction, schema)
        temps.add(currentTemp)
        var currentScan = currentTemp.open()
        while (copy(src, currentScan)) {
            if (comp.compare(src, currentScan) < 0) {
                // start a new run
                currentScan.close()
                currentTemp = TempTable(transaction, schema)
                temps.add(currentTemp)
                currentScan = currentTemp.open()
            }
        }
        currentScan.close()
        return temps
    }

    private fun doAMergeIteration(runs: MutableList<TempTable>): MutableList<TempTable> {
        val result = mutableListOf<TempTable>()
        while (runs.size > 1) {
            val p1 = runs.removeAt(0)
            val p2 = runs.removeAt(0)
            result.add(mergeTwoRuns(p1, p2))
        }
        if (runs.size == 1) {
            result.add(runs[0])
        }
        return result
    }

    private fun mergeTwoRuns(p1: TempTable, p2: TempTable): TempTable {
        val src1 = p1.open()
        val src2 = p2.open()
        val result = TempTable(transaction, schema)
        val dest = result.open()

        var hasMore1 = src1.next()
        var hasMore2 = src2.next()
        while (hasMore1 && hasMore2) {
            if (comp.compare(src1, src2) < 0) {
                hasMore1 = copy(src1, dest)
            } else {
                hasMore2 = copy(src2, dest)
            }
        }

        if (hasMore1) {
            while (hasMore1) {
                hasMore1 = copy(src1, dest)
            }
        } else {
            while (hasMore2) {
                hasMore2 = copy(src2, dest)
            }
        }
        src1.close()
        src2.close()
        dest.close()
        return result
    }

    private fun copy(src: Scan, dest: UpdateScan): Boolean {
        dest.insert()
        for (fieldName in schema.fields()) {
            dest.setVal(fieldName, src.getVal(fieldName))
        }
        return src.next()
    }
}
