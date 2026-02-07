package com.kura.multibuffer

import kotlin.math.ceil
import kotlin.math.pow

/**
 * A utility object containing methods which estimate
 * the optimal number of buffers to allocate for a scan.
 */
object BufferNeeds {

    /**
     * This method considers the various roots
     * of the specified output size (in blocks),
     * and returns the highest root that is less than
     * the number of available buffers.
     * @param available the number of available buffers
     * @param size the size of the output file (in blocks)
     * @return the highest root of size that fits in available buffers
     */
    fun bestRoot(available: Int, size: Int): Int {
        val avail = available - 2 // reserve a couple
        if (avail <= 1) return 1
        var k = Int.MAX_VALUE
        var i = 1.0
        while (k > avail) {
            i++
            k = ceil(size.toDouble().pow(1.0 / i)).toInt()
        }
        return k
    }

    /**
     * This method considers the various factors
     * of the specified output size (in blocks),
     * and returns the highest factor that is less than
     * the number of available buffers.
     * @param available the number of available buffers
     * @param size the size of the output file (in blocks)
     * @return the highest factor of size that fits in available buffers
     */
    fun bestFactor(available: Int, size: Int): Int {
        val avail = available - 2 // reserve a couple
        if (avail <= 1) return 1
        var k = size
        var i = 1.0
        while (k > avail) {
            i++
            k = ceil(size.toDouble() / i).toInt()
        }
        return k
    }
}
