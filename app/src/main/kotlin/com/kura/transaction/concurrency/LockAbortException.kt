package com.kura.transaction.concurrency

/**
 * A runtime exception indicating that the transaction
 * needs to abort because a lock could not be obtained.
 */
class LockAbortException : RuntimeException()