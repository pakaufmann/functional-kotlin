package com.pkaufmann.kotlin.ddd.application.domain

data class Amount(val amount: Int) {
    operator fun minus(other: Amount) =
        copy(amount = amount - other.amount)
}