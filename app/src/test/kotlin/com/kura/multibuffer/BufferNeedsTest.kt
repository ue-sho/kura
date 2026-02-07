package com.kura.multibuffer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class BufferNeedsTest {

    // bestRoot tests

    @Test
    fun `bestRoot should return 1 when available buffers is 3 or less`() {
        assertEquals(1, BufferNeeds.bestRoot(1, 100))
        assertEquals(1, BufferNeeds.bestRoot(2, 100))
        assertEquals(1, BufferNeeds.bestRoot(3, 100))
    }

    @Test
    fun `bestRoot should return square root for small sizes`() {
        // available=12, avail=10, size=10
        // i=2: ceil(10^(1/2)) = ceil(3.16) = 4 <= 10
        assertEquals(4, BufferNeeds.bestRoot(12, 10))
    }

    @Test
    fun `bestRoot should return square root when it fits`() {
        // available=12, avail=10, size=100
        // i=2: ceil(100^(1/2)) = ceil(10) = 10 <= 10
        assertEquals(10, BufferNeeds.bestRoot(12, 100))
    }

    @Test
    fun `bestRoot should use higher root when square root is too large`() {
        // available=7, avail=5, size=1000
        // i=2: ceil(sqrt(1000))=32 > 5
        // i=3: ceil(1000^(1/3))=10 > 5
        // i=4: ceil(1000^(1/4))=6 > 5
        // i=5: ceil(1000^(1/5))=4 <= 5
        assertEquals(4, BufferNeeds.bestRoot(7, 1000))
    }

    @Test
    fun `bestRoot should return 1 for size 1`() {
        assertEquals(1, BufferNeeds.bestRoot(10, 1))
    }

    // bestFactor tests

    @Test
    fun `bestFactor should return 1 when available buffers is 3 or less`() {
        assertEquals(1, BufferNeeds.bestFactor(1, 100))
        assertEquals(1, BufferNeeds.bestFactor(2, 100))
        assertEquals(1, BufferNeeds.bestFactor(3, 100))
    }

    @Test
    fun `bestFactor should return size when it fits in available buffers`() {
        // available=12, avail=10, size=10 -> 10 fits in 10
        assertEquals(10, BufferNeeds.bestFactor(12, 10))
    }

    @Test
    fun `bestFactor should return half of size when size is twice available`() {
        // available=7, avail=5, size=10 -> 10 > 5, ceil(10/2)=5 <= 5
        assertEquals(5, BufferNeeds.bestFactor(7, 10))
    }

    @Test
    fun `bestFactor should find the highest factor that fits`() {
        // available=5, avail=3, size=100
        // ceil(100/1)=100, ceil(100/2)=50, ..., ceil(100/34)=3 <= 3
        assertEquals(3, BufferNeeds.bestFactor(5, 100))
    }

    @Test
    fun `bestFactor should return 1 for size 1`() {
        assertEquals(1, BufferNeeds.bestFactor(10, 1))
    }

    @Test
    fun `bestFactor should handle large size values`() {
        // available=10, avail=8, size=1000 -> ceil(1000/125)=8
        assertEquals(8, BufferNeeds.bestFactor(10, 1000))
    }
}
