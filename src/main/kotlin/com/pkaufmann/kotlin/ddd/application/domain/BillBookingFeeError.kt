package com.pkaufmann.kotlin.ddd.application.domain

sealed class BillBookingFeeError

data class WalletNotFoundError(val userId: UserId) : BillBookingFeeError()