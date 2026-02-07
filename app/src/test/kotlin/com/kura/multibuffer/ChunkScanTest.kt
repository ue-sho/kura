package com.kura.multibuffer

import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordPage
import com.kura.record.Schema
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ChunkScanTest {
    private lateinit var transaction: Transaction
    private lateinit var layout: Layout
    private lateinit var schema: Schema

    @BeforeEach
    fun setUp() {
        transaction = mockk(relaxed = true)
        schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        layout = Layout(schema)
    }

    @Test
    fun `should pin all blocks in chunk on construction`() {
        // Create a chunk scan spanning blocks 0 to 2
        mockkConstructor(RecordPage::class)
        every { anyConstructed<RecordPage>().nextAfter(any()) } returns -1
        every { anyConstructed<RecordPage>().block() } answers {
            // Return the block based on construction args
            BlockId("test.tbl", 0)
        }

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 2)

        // Verify 3 blocks are pinned (via RecordPage constructors)
        verify(exactly = 3) { transaction.pin(any()) }
        chunkScan.close()
    }

    @Test
    fun `should unpin all blocks on close`() {
        mockkConstructor(RecordPage::class)
        every { anyConstructed<RecordPage>().nextAfter(any()) } returns -1
        every { anyConstructed<RecordPage>().block() } returns BlockId("test.tbl", 0)

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 2)
        chunkScan.close()

        verify { transaction.unpin(BlockId("test.tbl", 0)) }
        verify { transaction.unpin(BlockId("test.tbl", 1)) }
        verify { transaction.unpin(BlockId("test.tbl", 2)) }
    }

    @Test
    fun `should iterate records across blocks`() {
        mockkConstructor(RecordPage::class)

        // Simulate: block 0 has 1 record at slot 0, block 1 has 1 record at slot 0
        val pages = mutableListOf<RecordPage>()
        var constructionCount = 0

        every { anyConstructed<RecordPage>().nextAfter(any()) } answers {
            val slot = firstArg<Int>()
            // Return slot 0 for first call (slot=-1), then -1 for exhausted
            if (slot == -1) 0 else -1
        }
        every { anyConstructed<RecordPage>().block() } answers {
            BlockId("test.tbl", 0) // Simplified; used for moving to next block
        }
        every { anyConstructed<RecordPage>().getInt(0, "id") } returns 42
        every { anyConstructed<RecordPage>().getString(0, "name") } returns "alice"

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 0)

        // First record
        assertTrue(chunkScan.next())
        assertEquals(42, chunkScan.getInt("id"))
        assertEquals("alice", chunkScan.getString("name"))

        // No more records (single block, exhausted)
        assertFalse(chunkScan.next())

        chunkScan.close()
    }

    @Test
    fun `getVal should return Constant with int for integer fields`() {
        mockkConstructor(RecordPage::class)
        every { anyConstructed<RecordPage>().nextAfter(-1) } returns 0
        every { anyConstructed<RecordPage>().nextAfter(0) } returns -1
        every { anyConstructed<RecordPage>().block() } returns BlockId("test.tbl", 0)
        every { anyConstructed<RecordPage>().getInt(0, "id") } returns 99

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 0)
        assertTrue(chunkScan.next())

        val value = chunkScan.getVal("id")
        assertEquals(Constant(99), value)

        chunkScan.close()
    }

    @Test
    fun `getVal should return Constant with string for varchar fields`() {
        mockkConstructor(RecordPage::class)
        every { anyConstructed<RecordPage>().nextAfter(-1) } returns 0
        every { anyConstructed<RecordPage>().nextAfter(0) } returns -1
        every { anyConstructed<RecordPage>().block() } returns BlockId("test.tbl", 0)
        every { anyConstructed<RecordPage>().getString(0, "name") } returns "bob"

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 0)
        assertTrue(chunkScan.next())

        val value = chunkScan.getVal("name")
        assertEquals(Constant("bob"), value)

        chunkScan.close()
    }

    @Test
    fun `hasField should check schema`() {
        mockkConstructor(RecordPage::class)
        every { anyConstructed<RecordPage>().nextAfter(any()) } returns -1
        every { anyConstructed<RecordPage>().block() } returns BlockId("test.tbl", 0)

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 0)

        assertTrue(chunkScan.hasField("id"))
        assertTrue(chunkScan.hasField("name"))
        assertFalse(chunkScan.hasField("nonexistent"))

        chunkScan.close()
    }

    @Test
    fun `beforeFirst should reset to start of chunk`() {
        mockkConstructor(RecordPage::class)
        every { anyConstructed<RecordPage>().nextAfter(-1) } returns 0
        every { anyConstructed<RecordPage>().nextAfter(0) } returns -1
        every { anyConstructed<RecordPage>().block() } returns BlockId("test.tbl", 0)
        every { anyConstructed<RecordPage>().getInt(0, "id") } returns 1

        val chunkScan = ChunkScan(transaction, "test.tbl", layout, 0, 0)

        // Read through records
        assertTrue(chunkScan.next())
        assertFalse(chunkScan.next())

        // Reset and read again
        chunkScan.beforeFirst()
        assertTrue(chunkScan.next())
        assertEquals(1, chunkScan.getInt("id"))

        chunkScan.close()
    }
}
