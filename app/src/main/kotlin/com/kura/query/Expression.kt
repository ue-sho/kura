package com.kura.query

import com.kura.record.Schema

/**
 * The interface corresponding to SQL expressions.
 */
class Expression {
    private val value: Constant?
    private val fieldName: String?

    constructor(value: Constant) {
        this.value = value
        this.fieldName = null
    }

    constructor(fieldName: String) {
        this.value = null
        this.fieldName = fieldName
    }

    /**
     * Evaluate the expression with respect to the
     * current record of the specified scan.
     * @param scan the scan
     * @return the value of the expression, as a Constant
     */
    fun evaluate(scan: Scan): Constant {
        return if (value != null) value else scan.getVal(fieldName!!)
    }

    /**
     * Return true if the expression is a field reference.
     * @return true if the expression denotes a field
     */
    fun isFieldName(): Boolean {
        return fieldName != null
    }

    /**
     * Return true if the expression is a constant.
     * @return true if the expression denotes a constant
     */
    fun isConstant(): Boolean {
        return value != null
    }

    /**
     * Return the constant corresponding to a constant expression,
     * or null if the expression does not
     * denote a constant.
     * @return the expression as a constant
     */
    fun asConstant(): Constant? {
        return value
    }

    /**
     * Return the field name corresponding to a constant expression,
     * or null if the expression does not
     * denote a field.
     * @return the expression as a field name
     */
    fun asFieldName(): String? {
        return fieldName
    }

    /**
     * Determine if all of the fields mentioned in this expression
     * are contained in the specified schema.
     * @param schema the schema
     * @return true if all fields in the expression are in the schema
     */
    fun appliesTo(schema: Schema): Boolean {
        return value != null || schema.hasField(fieldName!!)
    }

    override fun toString(): String {
        return value?.toString() ?: fieldName!!
    }
}