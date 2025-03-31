package com.kura.plan

import com.kura.server.KuraDB
import kotlin.math.roundToInt

/**
 * Test for the Planner class with a single table query.
 */
class PlannerTest1 {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val db = KuraDB("plannertest1")
            val transaction = db.newTransaction()
            val planner = db.planner()
            if (planner == null) {
                throw RuntimeException("Planner is null")
            }

            val cmd = "create table T1(A int, B varchar(9))"
            planner.executeUpdate(cmd, transaction)

            val n = 200
            println("Inserting $n random records.")
            for (i in 0 until n) {
                val a = (Math.random() * 50).roundToInt()
                val b = "rec$a"
                val insertCmd = "insert into T1(A,B) values($a, '$b')"
                planner?.executeUpdate(insertCmd, transaction)
            }

            val qry = "select B from T1 where A=10"
            val plan = planner.createQueryPlan(qry, transaction)
            val scan = plan.open()
            while (scan.next()) {
                println(scan.getString("b"))
            }
            scan.close()
            transaction.commit()
        }
    }
}