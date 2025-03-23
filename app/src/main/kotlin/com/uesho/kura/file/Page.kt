package com.uesho.kura.file

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

/**
 * A page is a fixed-length sequence of bytes.
 * It provides methods to read and write values at any offset within the page.
 */
class Page {
    private val byteBuffer: ByteBuffer

    companion object {
        val CHARSET: Charset = StandardCharsets.US_ASCII

        /**
         * Calculate the maximum number of bytes needed to store a string of given length.
         *
         * @param strLen The length of the string
         * @return The maximum number of bytes needed
         */
        fun maxLength(strLen: Int): Int {
            val bytesPerChar = CHARSET.newEncoder().maxBytesPerChar()
            return Int.SIZE_BYTES + (strLen * bytesPerChar.toInt())
        }
    }

    /**
     * Create a new page with the specified block size.
     *
     * @param blockSize The size of the block in bytes
     */
    constructor(blockSize: Int) {
        byteBuffer = ByteBuffer.allocateDirect(blockSize)
    }

    /**
     * Create a new page from an existing byte array.
     *
     * @param bytes The byte array to create the page from
     */
    constructor(bytes: ByteArray) {
        byteBuffer = ByteBuffer.wrap(bytes)
    }

    /**
     * Get an integer value from the specified offset.
     *
     * @param offset The offset in the page
     * @return The integer value at the offset
     */
    fun getInt(offset: Int): Int = byteBuffer.getInt(offset)

    /**
     * Set an integer value at the specified offset.
     *
     * @param offset The offset in the page
     * @param value The value to set
     */
    fun setInt(offset: Int, value: Int) {
        byteBuffer.putInt(offset, value)
    }

    /**
     * Get a byte array from the specified offset.
     *
     * @param offset The offset in the page
     * @return The byte array at the offset
     */
    fun getBytes(offset: Int): ByteArray {
        byteBuffer.position(offset)
        val length = byteBuffer.getInt()
        val bytes = ByteArray(length)
        byteBuffer.get(bytes)
        return bytes
    }

    /**
     * Set a byte array at the specified offset.
     *
     * @param offset The offset in the page
     * @param bytes The byte array to set
     */
    fun setBytes(offset: Int, bytes: ByteArray) {
        byteBuffer.position(offset)
        byteBuffer.putInt(bytes.size)
        byteBuffer.put(bytes)
    }

    /**
     * Get a string from the specified offset.
     *
     * @param offset The offset in the page
     * @return The string at the offset
     */
    fun getString(offset: Int): String {
        val bytes = getBytes(offset)
        return String(bytes, CHARSET)
    }

    /**
     * Set a string at the specified offset.
     *
     * @param offset The offset in the page
     * @param value The string to set
     */
    fun setString(offset: Int, value: String) {
        val bytes = value.toByteArray(CHARSET)
        setBytes(offset, bytes)
    }

    /**
     * Get the contents of the page as a ByteBuffer.
     * This is package-private and used by FileMgr.
     */
    internal fun contents(): ByteBuffer {
        byteBuffer.position(0)
        return byteBuffer
    }
}