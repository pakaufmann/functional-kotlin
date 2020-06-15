package com.pkaufmann.kotlin.ddd.application.domain

data class BookingBilledMessage(val bookingId: BookingId, val amount: Amount)
