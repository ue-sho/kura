package com.kura.plan

import com.kura.server.KuraDB

/**
 * Test for the Planner class with a two-table query.
 */
class PlannerTest2 {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val db = KuraDB("plannertest2")
            val transaction = db.newTransaction()
            val planner = db.planner()
            if (planner == null) {
                throw RuntimeException("Planner is null")
            }

            val cmd = "create table T1(A int, B varchar(9))"
            planner.executeUpdate(cmd, transaction)
            val n = 200
            println("Inserting $n records into T1.")
            for (i in 0 until n) {
                val a = i
                val b = "bbb$a"
                val insertCmd = "insert into T1(A,B) values($a, '$b')"
                planner.executeUpdate(insertCmd, transaction)
            }

            val cmd2 = "create table T2(C int, D varchar(9))"
            planner.executeUpdate(cmd2, transaction)
            println("Inserting $n records into T2.")
            for (i in 0 until n) {
                val c = n - i - 1
                val d = "ddd$c"
                val insertCmd = "insert into T2(C,D) values($c, '$d')"
                planner.executeUpdate(insertCmd, transaction)
            }

            val qry = "select B,D from T1,T2 where A=C"
            val plan = planner.createQueryPlan(qry, transaction)
            val scan = plan.open()
            while (scan.next()) {
                println("${scan.getString("b")} ${scan.getString("d")}")
            }
            scan.close()
            transaction.commit()
        }
    }
}