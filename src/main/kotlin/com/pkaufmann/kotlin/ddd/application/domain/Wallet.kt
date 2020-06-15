package com.pkaufmann.kotlin.ddd.application.domain

data class Wallet(val userId: UserId, val amount: Amount) {
    fun billBookingFee(booking: Booking): Wallet =
        copy(amount = amount - Amount(1))
}