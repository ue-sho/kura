package com.kura.buffer

/**
 * A runtime exception indicating that the transaction
 * needs to abort because a buffer request could not be satisfied.
 */
class BufferAbortException : RuntimeException()