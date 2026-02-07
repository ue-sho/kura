package sample.sample_sql

import com.kura.plan.Planner
import com.kura.record.TableScan
import com.kura.server.KuraDB
import java.io.File
import kotlin.random.Random

/**
 * SampleSQL: A standalone program that creates multiple tables,
 * bulk-loads ~1,000,000 records, and runs various SQL queries
 * to verify the entire Kura stack (including materialize, multibuffer, opt).
 *
 * Usage:  ./gradlew runSampleSQL
 *
 * The program uses KuraDB directly (no RMI server needed).
 * Data directory: sampledb/ (auto-deleted on each run for a clean start)
 *
 * NOTE: Kura's parser (like SimpleDB) does NOT support "table.field" syntax.
 * Field names must be globally unique across all tables in a query.
 * So we use: dept.did, emp.edid (FK to dept), etc.
 */
object SampleSQL {

    // Larger block size and more buffers for bulk workload
    private const val BLOCK_SIZE = 4096
    private const val BUFFER_SIZE = 256
    private const val DB_DIR = "sampledb"

    // Data sizes
    private const val NUM_DEPARTMENTS = 50
    private const val NUM_EMPLOYEES = 1_000_000
    private const val NUM_PROJECTS = 200
    private const val NUM_ASSIGNMENTS = 2_000_000

    private val FIRST_NAMES = arrayOf(
        "alice", "bob", "carol", "dave", "eve",
        "frank", "grace", "heidi", "ivan", "judy",
        "ken", "lisa", "mike", "nancy", "oscar",
        "pat", "quinn", "ruth", "steve", "tina"
    )

    private val LAST_NAMES = arrayOf(
        "smith", "jones", "brown", "davis", "wilson",
        "moore", "taylor", "anderson", "thomas", "jackson"
    )

    private val DEPT_NAMES = arrayOf(
        "eng", "sales", "hr", "fin", "mktg",
        "ops", "legal", "rd", "qa", "support"
    )

    private val PROJECT_NAMES = arrayOf(
        "alpha", "beta", "gamma", "delta", "omega",
        "phoenix", "titan", "nova", "pulse", "apex"
    )

    @JvmStatic
    fun main(args: Array<String>) {
        // Clean start
        val dbDir = File(DB_DIR)
        if (dbDir.exists()) {
            println("Deleting existing database directory: $DB_DIR")
            dbDir.deleteRecursively()
        }

        println("=== SampleSQL: Kura Database Verification ===")
        println("Block size: $BLOCK_SIZE, Buffer pool: $BUFFER_SIZE buffers")
        println()

        val db = KuraDB(DB_DIR, BLOCK_SIZE, BUFFER_SIZE)

        // --- Phase 1: Schema creation ---
        println("--- Phase 1: Creating tables and indexes via SQL ---")
        val initTx = db.newTransaction()
        val isNew = db.fileManager().isNew()
        val mdm = com.kura.metadata.MetadataManager(isNew, initTx)
        val qp = com.kura.opt.HeuristicQueryPlanner(mdm)
        val up = com.kura.plan.BasicUpdatePlanner(mdm)
        val planner = Planner(qp, up)
        initTx.commit()

        createTables(db, planner)

        // --- Phase 2: Bulk load data (direct TableScan for speed) ---
        println()
        println("--- Phase 2: Bulk loading data ---")
        bulkLoadDepartments(db, mdm)
        bulkLoadEmployees(db, mdm)
        bulkLoadProjects(db, mdm)
        bulkLoadAssignments(db, mdm)

        // --- Phase 3: Create indexes ---
        println()
        println("--- Phase 3: Creating indexes ---")
        createIndexes(db, planner)

        // --- Phase 4: Run SQL queries ---
        println()
        println("--- Phase 4: Running SQL queries ---")
        runQueries(db, planner)

        println()
        println("=== All queries completed successfully! ===")

        // Cleanup
        println("Deleting database directory: $DB_DIR")
        dbDir.deleteRecursively()
    }

    // ----------------------------------------------------------------
    // Schema creation
    // ----------------------------------------------------------------

    private fun createTables(db: KuraDB, planner: Planner) {
        val tx = db.newTransaction()

        // Field names are globally unique to avoid ambiguity in joins:
        //   dept:       did (PK), dname, dfloor
        //   emp:        eid (PK), ename, salary, edid (FK -> dept.did)
        //   project:    pid (PK), pname, budget
        //   assignment: aeid (FK -> emp.eid), apid (FK -> project.pid), role
        val ddls = listOf(
            "create table dept (did int, dname varchar(10), dfloor int)",
            "create table emp (eid int, ename varchar(15), salary int, edid int)",
            "create table project (pid int, pname varchar(15), budget int)",
            "create table assignment (aeid int, apid int, role varchar(10))"
        )

        for (ddl in ddls) {
            val start = System.currentTimeMillis()
            planner.executeUpdate(ddl, tx)
            val elapsed = System.currentTimeMillis() - start
            println("  $ddl  (${elapsed}ms)")
        }

        tx.commit()
    }

    private fun createIndexes(db: KuraDB, planner: Planner) {
        val tx = db.newTransaction()

        val ddls = listOf(
            "create index idx_emp_edid on emp (edid)",
            "create index idx_emp_salary on emp (salary)",
            "create index idx_asgn_aeid on assignment (aeid)",
            "create index idx_asgn_apid on assignment (apid)"
        )

        for (ddl in ddls) {
            val start = System.currentTimeMillis()
            planner.executeUpdate(ddl, tx)
            val elapsed = System.currentTimeMillis() - start
            println("  $ddl  (${elapsed}ms)")
        }

        tx.commit()
    }

    // ----------------------------------------------------------------
    // Bulk loading (direct TableScan bypass for performance)
    // ----------------------------------------------------------------

    private fun bulkLoadDepartments(db: KuraDB, mdm: com.kura.metadata.MetadataManager) {
        val start = System.currentTimeMillis()
        val tx = db.newTransaction()
        val layout = mdm.getLayout("dept", tx)
        val ts = TableScan(tx, "dept", layout)
        val rng = Random(42)

        for (i in 1..NUM_DEPARTMENTS) {
            ts.insert()
            ts.setInt("did", i)
            ts.setString("dname", DEPT_NAMES[i % DEPT_NAMES.size])
            ts.setInt("dfloor", rng.nextInt(1, 30))
        }

        ts.close()
        tx.commit()
        val elapsed = System.currentTimeMillis() - start
        println("  dept: $NUM_DEPARTMENTS rows loaded (${elapsed}ms)")
    }

    private fun bulkLoadEmployees(db: KuraDB, mdm: com.kura.metadata.MetadataManager) {
        val start = System.currentTimeMillis()
        val rng = Random(123)

        // Commit in batches to avoid buffer starvation
        var loaded = 0
        val batchSize = 50_000
        while (loaded < NUM_EMPLOYEES) {
            val tx = db.newTransaction()
            val layout = mdm.getLayout("emp", tx)
            val ts = TableScan(tx, "emp", layout)
            val end = minOf(loaded + batchSize, NUM_EMPLOYEES)
            for (i in (loaded + 1)..end) {
                ts.insert()
                ts.setInt("eid", i)
                val name = FIRST_NAMES[rng.nextInt(FIRST_NAMES.size)] +
                    LAST_NAMES[rng.nextInt(LAST_NAMES.size)]
                ts.setString("ename", name.take(15))
                ts.setInt("salary", rng.nextInt(30000, 150000))
                ts.setInt("edid", rng.nextInt(1, NUM_DEPARTMENTS + 1))
            }
            ts.close()
            tx.commit()
            loaded = end
            if (loaded % 200_000 == 0 || loaded == NUM_EMPLOYEES) {
                val elapsed = System.currentTimeMillis() - start
                println("  emp: $loaded / $NUM_EMPLOYEES rows loaded (${elapsed}ms)")
            }
        }
    }

    private fun bulkLoadProjects(db: KuraDB, mdm: com.kura.metadata.MetadataManager) {
        val start = System.currentTimeMillis()
        val tx = db.newTransaction()
        val layout = mdm.getLayout("project", tx)
        val ts = TableScan(tx, "project", layout)
        val rng = Random(456)

        for (i in 1..NUM_PROJECTS) {
            ts.insert()
            ts.setInt("pid", i)
            ts.setString("pname", PROJECT_NAMES[i % PROJECT_NAMES.size])
            ts.setInt("budget", rng.nextInt(10000, 1_000_000))
        }

        ts.close()
        tx.commit()
        val elapsed = System.currentTimeMillis() - start
        println("  project: $NUM_PROJECTS rows loaded (${elapsed}ms)")
    }

    private fun bulkLoadAssignments(db: KuraDB, mdm: com.kura.metadata.MetadataManager) {
        val start = System.currentTimeMillis()
        val rng = Random(789)
        val roles = arrayOf("lead", "dev", "test", "pm", "analyst")

        var loaded = 0
        val batchSize = 50_000
        while (loaded < NUM_ASSIGNMENTS) {
            val tx = db.newTransaction()
            val layout = mdm.getLayout("assignment", tx)
            val ts = TableScan(tx, "assignment", layout)
            val end = minOf(loaded + batchSize, NUM_ASSIGNMENTS)
            for (i in (loaded + 1)..end) {
                ts.insert()
                ts.setInt("aeid", rng.nextInt(1, NUM_EMPLOYEES + 1))
                ts.setInt("apid", rng.nextInt(1, NUM_PROJECTS + 1))
                ts.setString("role", roles[rng.nextInt(roles.size)])
            }
            ts.close()
            tx.commit()
            loaded = end
            if (loaded % 500_000 == 0 || loaded == NUM_ASSIGNMENTS) {
                val elapsed = System.currentTimeMillis() - start
                println("  assignment: $loaded / $NUM_ASSIGNMENTS rows loaded (${elapsed}ms)")
            }
        }
    }

    // ----------------------------------------------------------------
    // SQL queries
    // ----------------------------------------------------------------

    private fun runQueries(db: KuraDB, planner: Planner) {
        // Q1: Simple select with predicate on a single table
        runQuery(
            db, planner,
            "Q1: Select a single department by id",
            "select dname, dfloor from dept where did = 1"
        )

        // Q2: Full scan on large table with equality predicate
        runQuery(
            db, planner,
            "Q2: Employees with a specific salary (full scan, 1M rows)",
            "select eid, ename, salary from emp where salary = 75000"
        )

        // Q3: Point lookup on large table
        runQuery(
            db, planner,
            "Q3: Single employee lookup by id",
            "select eid, ename, salary, edid from emp where eid = 500000"
        )

        // Q4: Two-table join (emp x dept) — product with selection
        //     Small x large product filtered by did and eid range
        runQuery(
            db, planner,
            "Q4: Dept x Emp product (did=1, eid=1)",
            "select ename, salary, dname from emp, dept where did = 1 and eid = 1"
        )

        // Q5: Two-table join (assignment x project) — product with filter
        runQuery(
            db, planner,
            "Q5: Assignment x Project product (apid=1, pid=1)",
            "select aeid, role, pname from assignment, project where apid = 1 and pid = 1"
        )

        // Q6: Three-table product (emp x dept x assignment) — with filters to keep small
        runQuery(
            db, planner,
            "Q6: Three-table product (emp+dept+assignment) with filters",
            "select ename, dname, role from emp, dept, assignment where eid = 42 and did = 1 and aeid = 42"
        )

        // Q7: Single-table select on project
        runQuery(
            db, planner,
            "Q7: Project lookup by id",
            "select pname, budget from project where pid = 100"
        )

        // Q8: Two-table product (small x small) — dept x project
        runQuery(
            db, planner,
            "Q8: Dept x Project product with filters (did=1, pid=1)",
            "select dname, pname, budget from dept, project where did = 1 and pid = 1"
        )
    }

    private fun runQuery(db: KuraDB, planner: Planner, label: String, sql: String) {
        println()
        println("  $label")
        println("  SQL: $sql")

        val tx = db.newTransaction()
        try {
            val start = System.currentTimeMillis()
            val plan = planner.createQueryPlan(sql, tx)
            val scan = plan.open()

            var count = 0
            val maxDisplay = 20
            while (scan.next()) {
                count++
                if (count <= maxDisplay) {
                    val fields = plan.schema().fields()
                    val row = fields.joinToString(" | ") { fieldName ->
                        try {
                            scan.getVal(fieldName).toString()
                        } catch (e: Exception) {
                            "ERR"
                        }
                    }
                    println("    $row")
                }
            }
            if (count > maxDisplay) {
                println("    ... (${count - maxDisplay} more rows)")
            }

            scan.close()
            val elapsed = System.currentTimeMillis() - start
            println("  => $count rows returned (${elapsed}ms)")
        } catch (e: Exception) {
            println("  => ERROR: ${e.message}")
            e.printStackTrace()
        } finally {
            tx.commit()
        }
    }
}
