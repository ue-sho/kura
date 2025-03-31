package com.kura.query

/**
 * The class that denotes values stored in the database.
 */
class Constant : Comparable<Constant> {
    private val intVal: Int?
    private val strVal: String?

    constructor(intVal: Int) {
        this.intVal = intVal
        this.strVal = null
    }

    constructor(strVal: String) {
        this.intVal = null
        this.strVal = strVal
    }

    /**
     * Returns the integer value of this constant.
     * @return the integer value
     */
    fun intValue(): Int {
        return intVal ?: 0
    }

    /**
     * Returns the string value of this constant.
     * @return the string value
     */
    fun stringValue(): String {
        return strVal ?: ""
    }

    fun asInt(): Int {
        return intVal ?: 0
    }

    fun asString(): String {
        return strVal ?: ""
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Constant) return false
        return if (intVal != null) {
            intVal == other.intVal
        } else {
            strVal == other.strVal
        }
    }

    override fun compareTo(other: Constant): Int {
        return if (intVal != null && other.intVal != null) {
            intVal.compareTo(other.intVal)
        } else if (strVal != null && other.strVal != null) {
            strVal.compareTo(other.strVal)
        } else {
            0
        }
    }

    override fun hashCode(): Int {
        return intVal?.hashCode() ?: strVal?.hashCode() ?: 0
    }

    override fun toString(): String {
        return intVal?.toString() ?: strVal ?: ""
    }
}