package com.kura.index.query

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.index.hash.HashIndex
import com.kura.log.LogManager
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class IndexJoinScanTest {
    @TempDir
    lateinit var tempDir: File

    private lateinit var fm: FileManager
    private lateinit var lm: LogManager
    private lateinit var bm: BufferManager
    private val blockSize = 400

    @BeforeEach
    fun setUp() {
        fm = FileManager(tempDir, blockSize)
        lm = LogManager(fm, "testlog")
        bm = BufferManager(fm, lm, 8)
    }

    @Test
    fun `should join LHS records with matching RHS records via index`() {
        val tx = Transaction(fm, lm, bm)

        // Create "student" table (LHS): id, name
        val studentSchema = Schema()
        studentSchema.addIntField("sid")
        studentSchema.addStringField("sname", 10)
        val studentLayout = Layout(studentSchema)

        val sts = TableScan(tx, "student", studentLayout)
        sts.insert(); sts.setInt("sid", 1); sts.setString("sname", "alice")
        sts.insert(); sts.setInt("sid", 2); sts.setString("sname", "bob")
        sts.insert(); sts.setInt("sid", 3); sts.setString("sname", "carol")
        sts.close()

        // Create "enroll" table (RHS): studentid, grade
        val enrollSchema = Schema()
        enrollSchema.addIntField("studentid")
        enrollSchema.addStringField("grade", 2)
        val enrollLayout = Layout(enrollSchema)

        val ets = TableScan(tx, "enroll", enrollLayout)
        ets.insert(); ets.setInt("studentid", 1); ets.setString("grade", "A")
        val rid1 = ets.getRecordId()
        ets.insert(); ets.setInt("studentid", 1); ets.setString("grade", "B")
        val rid2 = ets.getRecordId()
        ets.insert(); ets.setInt("studentid", 2); ets.setString("grade", "C")
        val rid3 = ets.getRecordId()
        ets.close()

        // Build index on enroll.studentid
        val idxLayout = Layout(Schema().apply {
            addIntField("block")
            addIntField("id")
            addIntField("dataval")
        })
        val idx = HashIndex(tx, "enrollidx", idxLayout)
        idx.insert(Constant(1), rid1)
        idx.insert(Constant(1), rid2)
        idx.insert(Constant(2), rid3)
        idx.close()

        // IndexJoinScan: student JOIN enroll ON student.sid = enroll.studentid
        val lhs = TableScan(tx, "student", studentLayout)
        val rhs = TableScan(tx, "enroll", enrollLayout)
        val joinIdx = HashIndex(tx, "enrollidx", idxLayout)
        val scan = IndexJoinScan(lhs, joinIdx, "sid", rhs)

        val results = mutableListOf<String>()
        while (scan.next()) {
            val name = scan.getString("sname")
            val grade = scan.getString("grade")
            results.add("$name:$grade")
        }

        // alice has 2 enrollments, bob has 1, carol has 0
        assertEquals(3, results.size)
        assertTrue(results.any { it.startsWith("alice:") })
        assertTrue(results.any { it.startsWith("bob:") })
        // carol (sid=3) has no enrollment, so should not appear
        assertFalse(results.any { it.startsWith("carol:") })

        scan.close()
        tx.commit()
    }

    @Test
    fun `should return no results when no matches exist`() {
        val tx = Transaction(fm, lm, bm)

        // LHS table
        val lhsSchema = Schema()
        lhsSchema.addIntField("key")
        val lhsLayout = Layout(lhsSchema)
        val lhsTs = TableScan(tx, "lhstable", lhsLayout)
        lhsTs.insert(); lhsTs.setInt("key", 999)
        lhsTs.close()

        // RHS table (empty index)
        val rhsSchema = Schema()
        rhsSchema.addIntField("fk")
        rhsSchema.addStringField("val", 5)
        val rhsLayout = Layout(rhsSchema)
        // Insert a record so table file exists, but don't index it with key 999
        val rhsTs = TableScan(tx, "rhstable", rhsLayout)
        rhsTs.insert(); rhsTs.setInt("fk", 1); rhsTs.setString("val", "x")
        val rid = rhsTs.getRecordId()
        rhsTs.close()

        val idxLayout = Layout(Schema().apply {
            addIntField("block")
            addIntField("id")
            addIntField("dataval")
        })
        val idx = HashIndex(tx, "fkidx", idxLayout)
        idx.insert(Constant(1), rid)
        idx.close()

        val lhs = TableScan(tx, "lhstable", lhsLayout)
        val rhs = TableScan(tx, "rhstable", rhsLayout)
        val joinIdx = HashIndex(tx, "fkidx", idxLayout)
        val scan = IndexJoinScan(lhs, joinIdx, "key", rhs)

        assertFalse(scan.next())

        scan.close()
        tx.commit()
    }

    @Test
    fun `should support hasField from both LHS and RHS`() {
        val tx = Transaction(fm, lm, bm)

        val lhsSchema = Schema()
        lhsSchema.addIntField("lid")
        val lhsLayout = Layout(lhsSchema)
        val lhsTs = TableScan(tx, "lhs2", lhsLayout)
        lhsTs.insert(); lhsTs.setInt("lid", 1)
        lhsTs.close()

        val rhsSchema = Schema()
        rhsSchema.addIntField("rid")
        rhsSchema.addStringField("rval", 5)
        val rhsLayout = Layout(rhsSchema)
        val rhsTs = TableScan(tx, "rhs2", rhsLayout)
        rhsTs.insert(); rhsTs.setInt("rid", 1); rhsTs.setString("rval", "a")
        val rid = rhsTs.getRecordId()
        rhsTs.close()

        val idxLayout = Layout(Schema().apply {
            addIntField("block")
            addIntField("id")
            addIntField("dataval")
        })
        val idx = HashIndex(tx, "ridx2", idxLayout)
        idx.insert(Constant(1), rid)
        idx.close()

        val lhs = TableScan(tx, "lhs2", lhsLayout)
        val rhs = TableScan(tx, "rhs2", rhsLayout)
        val joinIdx = HashIndex(tx, "ridx2", idxLayout)
        val scan = IndexJoinScan(lhs, joinIdx, "lid", rhs)

        assertTrue(scan.hasField("lid"))  // LHS field
        assertTrue(scan.hasField("rid"))  // RHS field
        assertTrue(scan.hasField("rval")) // RHS field
        assertFalse(scan.hasField("nonexistent"))

        scan.close()
        tx.commit()
    }
}
