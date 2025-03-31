package com.kura.query

import com.kura.plan.Plan
import com.kura.record.Schema

/**
 * A predicate is a Boolean combination of terms.
 */
class Predicate {
    private val terms: MutableList<Term> = ArrayList()

    /**
     * Create an empty predicate, corresponding to "true".
     */
    constructor()

    /**
     * Create a predicate containing a single term.
     * @param term the term
     */
    constructor(term: Term) {
        terms.add(term)
    }

    /**
     * Modifies the predicate to be the conjunction of
     * itself and the specified predicate.
     * @param predicate the other predicate
     */
    fun conjoinWith(predicate: Predicate) {
        terms.addAll(predicate.terms)
    }

    /**
     * Returns true if the predicate evaluates to true
     * with respect to the specified scan.
     * @param scan the scan
     * @return true if the predicate is true in the scan
     */
    fun isSatisfied(scan: Scan): Boolean {
        for (term in terms) {
            if (!term.isSatisfied(scan)) {
                return false
            }
        }
        return true
    }

    /**
     * Calculate the extent to which selecting on the predicate
     * reduces the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * predicate cuts the size of the output in half.
     * @param plan the query's plan
     * @return the integer reduction factor.
     */
    fun reductionFactor(plan: Plan): Int {
        var factor = 1
        for (term in terms) {
            factor *= term.reductionFactor(plan)
        }
        return factor
    }

    /**
     * Return the subpredicate that applies to the specified schema.
     * @param schema the schema
     * @return the subpredicate applying to the schema
     */
    fun selectSubPredicate(schema: Schema): Predicate? {
        val result = Predicate()
        for (term in terms) {
            if (term.appliesTo(schema)) {
                result.terms.add(term)
            }
        }
        return if (result.terms.isEmpty()) null else result
    }

    /**
     * Return the subpredicate consisting of terms that apply
     * to the union of the two specified schemas,
     * but not to either schema separately.
     * @param schema1 the first schema
     * @param schema2 the second schema
     * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
     */
    fun joinSubPredicate(schema1: Schema, schema2: Schema): Predicate? {
        val result = Predicate()
        val newSchema = Schema()
        newSchema.addAll(schema1)
        newSchema.addAll(schema2)
        for (term in terms) {
            if (!term.appliesTo(schema1) &&
                !term.appliesTo(schema2) &&
                term.appliesTo(newSchema)
            ) {
                result.terms.add(term)
            }
        }
        return if (result.terms.isEmpty()) null else result
    }

    /**
     * Determine if there is a term of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     * @param fieldName the name of the field
     * @return either the constant or null
     */
    fun equatesWithConstant(fieldName: String): Constant? {
        for (term in terms) {
            val constant = term.equatesWithConstant(fieldName)
            if (constant != null) {
                return constant
            }
        }
        return null
    }

    /**
     * Determine if there is a term of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fieldName the name of the field
     * @return the name of the other field, or null
     */
    fun equatesWithField(fieldName: String): String? {
        for (term in terms) {
            val otherField = term.equatesWithField(fieldName)
            if (otherField != null) {
                return otherField
            }
        }
        return null
    }

    override fun toString(): String {
        if (terms.isEmpty()) {
            return ""
        }
        val result = StringBuilder(terms[0].toString())
        for (i in 1 until terms.size) {
            result.append(" and ${terms[i]}")
        }
        return result.toString()
    }
}