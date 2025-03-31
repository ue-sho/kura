package com.kura.query

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import io.mockk.mockk
import io.mockk.every
import io.mockk.verify

class ProductTest {

    @Test
    fun `should correctly join records from two tables using product scan`() {
        // Create basic test data
        val data1 = arrayOf(
            mapOf("A" to 1, "B" to "aaa1"),
            mapOf("A" to 2, "B" to "aaa2")
        )

        val data2 = arrayOf(
            mapOf("C" to 10, "D" to "bbb10"),
            mapOf("C" to 20, "D" to "bbb20"),
            mapOf("C" to 30, "D" to "bbb30")
        )

        // Create and configure the mock scans
        val mockScan1 = mockk<Scan>(relaxed = true)
        val mockScan2 = mockk<Scan>(relaxed = true)

        // Setup field metadata
        every { mockScan1.hasField("A") } returns true
        every { mockScan1.hasField("B") } returns true
        every { mockScan1.hasField("C") } returns false
        every { mockScan1.hasField("D") } returns false

        every { mockScan2.hasField("A") } returns false
        every { mockScan2.hasField("B") } returns false
        every { mockScan2.hasField("C") } returns true
        every { mockScan2.hasField("D") } returns true

        // Setup traversal state
        var scan1Idx = -1
        var scan2Idx = 0
        var scan1Moved = false

        // Implement the scan1.next() logic
        every { mockScan1.next() } answers {
            if (!scan1Moved) {
                scan1Idx = 0
                scan1Moved = true
                return@answers true
            }

            if (scan1Idx < data1.size - 1) {
                scan1Idx++
                return@answers true
            }

            false
        }

        // Implement the scan2.next() logic
        every { mockScan2.next() } answers {
            if (scan2Idx < data2.size) {
                scan2Idx++
                return@answers true
            }
            scan2Idx = 0
            false
        }

        // Setup data getters for scan1
        every { mockScan1.getInt("A") } answers { data1[scan1Idx]["A"] as Int }
        every { mockScan1.getString("B") } answers { data1[scan1Idx]["B"] as String }

        // Setup data getters for scan2
        every { mockScan2.getInt("C") } answers { data2[scan2Idx - 1]["C"] as Int }
        every { mockScan2.getString("D") } answers { data2[scan2Idx - 1]["D"] as String }

        // Create the ProductScan
        val productScan = ProductScan(mockScan1, mockScan2)

        // Act & Assert: First record
        assertTrue(productScan.next(), "Should return first record")
        assertEquals(1, productScan.getInt("A"))
        assertEquals("aaa1", productScan.getString("B"))
        assertEquals(10, productScan.getInt("C"))
        assertEquals("bbb10", productScan.getString("D"))

        // Second record
        assertTrue(productScan.next(), "Should return second record")
        assertEquals(1, productScan.getInt("A"))
        assertEquals("aaa1", productScan.getString("B"))
        assertEquals(20, productScan.getInt("C"))
        assertEquals("bbb20", productScan.getString("D"))

        // Third record
        assertTrue(productScan.next(), "Should return third record")
        assertEquals(1, productScan.getInt("A"))
        assertEquals("aaa1", productScan.getString("B"))
        assertEquals(30, productScan.getInt("C"))
        assertEquals("bbb30", productScan.getString("D"))

        // Fourth record
        assertTrue(productScan.next(), "Should return fourth record")
        assertEquals(2, productScan.getInt("A"))
        assertEquals("aaa2", productScan.getString("B"))
        assertEquals(10, productScan.getInt("C"))
        assertEquals("bbb10", productScan.getString("D"))

        // Fifth record
        assertTrue(productScan.next(), "Should return fifth record")
        assertEquals(2, productScan.getInt("A"))
        assertEquals("aaa2", productScan.getString("B"))
        assertEquals(20, productScan.getInt("C"))
        assertEquals("bbb20", productScan.getString("D"))

        // Sixth record
        assertTrue(productScan.next(), "Should return sixth record")
        assertEquals(2, productScan.getInt("A"))
        assertEquals("aaa2", productScan.getString("B"))
        assertEquals(30, productScan.getInt("C"))
        assertEquals("bbb30", productScan.getString("D"))

        // No more records
        assertEquals(false, productScan.next(), "Should not have any more records")

        // Verify beforeFirst was called
        verify { mockScan1.beforeFirst() }
        verify { mockScan2.beforeFirst() }
    }
}