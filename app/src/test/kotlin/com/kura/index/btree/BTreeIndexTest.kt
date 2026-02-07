package com.kura.index.btree

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.record.Schema
import com.kura.transaction.Transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class BTreeIndexTest {
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

    private fun createLeafLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        schema.addIntField("dataval")
        return Layout(schema)
    }

    private fun createStringLeafLayout(fieldLength: Int): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        schema.addStringField("dataval", fieldLength)
        return Layout(schema)
    }

    @Test
    fun `should insert and retrieve single record`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val index = BTreeIndex(tx, "testidx", layout)

        val key = Constant(42)
        val rid = RecordId(1, 5)
        index.insert(key, rid)

        index.beforeFirst(key)
        assertTrue(index.next())
        assertEquals(rid, index.getDataRecordId())
        assertFalse(index.next())

        index.close()
        tx.commit()
    }

    @Test
    fun `should insert and retrieve multiple records`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val index = BTreeIndex(tx, "testidx2", layout)

        // Insert records with different keys
        for (i in 0 until 10) {
            index.insert(Constant(i * 10), RecordId(i, 0))
        }

        // Search for a specific key
        index.beforeFirst(Constant(50))
        assertTrue(index.next())
        val rid = index.getDataRecordId()
        assertEquals(5, rid.blockNumber())
        assertEquals(0, rid.slot())
        assertFalse(index.next())

        index.close()
        tx.commit()
    }

    @Test
    fun `should handle duplicate keys`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val index = BTreeIndex(tx, "testidx3", layout)

        // Insert multiple records with the same key
        val key = Constant(100)
        index.insert(key, RecordId(1, 0))
        index.insert(key, RecordId(2, 0))
        index.insert(key, RecordId(3, 0))

        // All three records should be found
        index.beforeFirst(key)
        val results = mutableListOf<RecordId>()
        while (index.next()) {
            results.add(index.getDataRecordId())
        }
        assertEquals(3, results.size)

        index.close()
        tx.commit()
    }

    @Test
    fun `should delete a record`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val index = BTreeIndex(tx, "testidx4", layout)

        val key = Constant(42)
        val rid1 = RecordId(1, 0)
        val rid2 = RecordId(2, 0)
        index.insert(key, rid1)
        index.insert(key, rid2)

        // Delete one of the records
        index.delete(key, rid1)

        // Only one record should remain
        index.beforeFirst(key)
        val results = mutableListOf<RecordId>()
        while (index.next()) {
            results.add(index.getDataRecordId())
        }
        assertEquals(1, results.size)
        assertEquals(rid2, results[0])

        index.close()
        tx.commit()
    }

    @Test
    fun `should return no results for non-existent key`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val index = BTreeIndex(tx, "testidx5", layout)

        index.insert(Constant(10), RecordId(1, 0))

        index.beforeFirst(Constant(99))
        assertFalse(index.next())

        index.close()
        tx.commit()
    }

    @Test
    fun `should work with string keys`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createStringLeafLayout(10)
        val index = BTreeIndex(tx, "testidxstr", layout)

        index.insert(Constant("alice"), RecordId(1, 0))
        index.insert(Constant("bob"), RecordId(2, 0))
        index.insert(Constant("charlie"), RecordId(3, 0))

        index.beforeFirst(Constant("bob"))
        assertTrue(index.next())
        val rid = index.getDataRecordId()
        assertEquals(2, rid.blockNumber())
        assertEquals(0, rid.slot())
        assertFalse(index.next())

        index.close()
        tx.commit()
    }

    @Test
    fun `searchCost should return expected value`() {
        // With 100 blocks and 10 records per block:
        // 1 + log(100)/log(10) = 1 + 2 = 3
        assertEquals(3, BTreeIndex.searchCost(100, 10))

        // With 1 block: 1 + log(1)/log(x) = 1 + 0 = 1
        assertEquals(1, BTreeIndex.searchCost(1, 10))
    }

    @Test
    fun `should handle many inserts causing splits`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val index = BTreeIndex(tx, "testidx6", layout)

        // Insert enough records to trigger page splits
        val numRecords = 50
        for (i in 0 until numRecords) {
            index.insert(Constant(i), RecordId(i, 0))
        }

        // Verify all records can be found
        for (i in 0 until numRecords) {
            index.beforeFirst(Constant(i))
            assertTrue(index.next(), "Should find record with key $i")
            val rid = index.getDataRecordId()
            assertEquals(i, rid.blockNumber())
            assertEquals(0, rid.slot())
            assertFalse(index.next())
        }

        index.close()
        tx.commit()
    }
}
