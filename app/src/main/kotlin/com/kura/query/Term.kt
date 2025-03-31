package com.kura.query

import com.kura.plan.Plan
import com.kura.record.Schema

/**
 * A term is a comparison between two expressions.
 */
class Term(
    private val lhs: Expression,
    private val rhs: Expression
) {
    /**
     * Returns the left-hand side expression of this term.
     * @return the left-hand side expression
     */
    fun lhs(): Expression {
        return lhs
    }

    /**
     * Returns the right-hand side expression of this term.
     * @return the right-hand side expression
     */
    fun rhs(): Expression {
        return rhs
    }

    /**
     * Return true if both of the term's expressions
     * evaluate to the same constant,
     * with respect to the specified scan.
     * @param scan the scan
     * @return true if both expressions have the same value in the scan
     */
    fun isSatisfied(scan: Scan): Boolean {
        val lhsVal = lhs.evaluate(scan)
        val rhsVal = rhs.evaluate(scan)
        return rhsVal == lhsVal
    }

    /**
     * Calculate the extent to which selecting on the term reduces
     * the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * term cuts the size of the output in half.
     * @param plan the query's plan
     * @return the integer reduction factor.
     */
    fun reductionFactor(plan: Plan): Int {
        if (lhs.isFieldName() && rhs.isFieldName()) {
            val lhsName = lhs.asFieldName()!!
            val rhsName = rhs.asFieldName()!!
            return maxOf(plan.distinctValues(lhsName), plan.distinctValues(rhsName))
        }
        if (lhs.isFieldName()) {
            val lhsName = lhs.asFieldName()!!
            return plan.distinctValues(lhsName)
        }
        if (rhs.isFieldName()) {
            val rhsName = rhs.asFieldName()!!
            return plan.distinctValues(rhsName)
        }
        // otherwise, the term equates constants
        return if (lhs.asConstant() == rhs.asConstant()) 1 else Int.MAX_VALUE
    }

    /**
     * Determine if this term is of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     * @param fieldName the name of the field
     * @return either the constant or null
     */
    fun equatesWithConstant(fieldName: String): Constant? {
        if (lhs.isFieldName() &&
            lhs.asFieldName() == fieldName &&
            !rhs.isFieldName()
        ) {
            return rhs.asConstant()
        } else if (rhs.isFieldName() &&
            rhs.asFieldName() == fieldName &&
            !lhs.isFieldName()
        ) {
            return lhs.asConstant()
        }
        return null
    }

    /**
     * Determine if this term is of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fieldName the name of the field
     * @return either the name of the other field, or null
     */
    fun equatesWithField(fieldName: String): String? {
        if (lhs.isFieldName() &&
            lhs.asFieldName() == fieldName &&
            rhs.isFieldName()
        ) {
            return rhs.asFieldName()
        } else if (rhs.isFieldName() &&
            rhs.asFieldName() == fieldName &&
            lhs.isFieldName()
        ) {
            return lhs.asFieldName()
        }
        return null
    }

    /**
     * Return true if both of the term's expressions
     * apply to the specified schema.
     * @param schema the schema
     * @return true if both expressions apply to the schema
     */
    fun appliesTo(schema: Schema): Boolean {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema)
    }

    override fun toString(): String {
        return "${lhs}=${rhs}"
    }
}