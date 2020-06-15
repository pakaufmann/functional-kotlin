package com.pkaufmann.kotlin.ddd.application

import arrow.core.*
import arrow.core.extensions.id.applicative.just
import arrow.core.extensions.id.monad.monad
import arrow.mtl.EitherT
import com.pkaufmann.kotlin.ddd.application.domain.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

fun <I, F, R> I.check(m: F, ret: R): Id<R> =
    when (this) {
        m -> ret.just()
        else -> throw NotImplementedError()
    }

fun <I, F> errorOnInvoke(): (I) -> F = { _ -> throw NotImplementedError() }

class WalletServiceTest {
    @Test
    fun `bill booking fee works`() {
        val id = Id.monad()

        val bill = WalletService.billBookingFee(
            id,
            { EitherT(it.check(UserId("1"), Right(Wallet(it, Amount(2))))) },
            { EitherT(it.check(Wallet(UserId("1"), Amount(1)), Either.Right(Unit))) },
            { it.check(BookingBilledMessage(BookingId("1"), Amount(1)), Unit) }
        )

        val result = bill(Booking(BookingId("1"), UserId("1"))).value().value()

        assertEquals(Right(Amount(1)), result)
    }

    @Test
    fun `bill booking fee should return an error when the wallet does not exist`() {
        val id = Id.monad()

        val bill = WalletService.billBookingFee(
            id,
            { EitherT.left(id, WalletNotFoundError(it)) },
            errorOnInvoke(),
            errorOnInvoke()
        )

        val result = bill(Booking(BookingId("1"), UserId("1"))).value().value()

        assertEquals(Left(WalletNotFoundError(UserId("1"))), result)

    }

    @Test
    fun `bill booking fee should return an error when the wallet could not be updated`() {
        val id = Id.monad()

        val bill = WalletService.billBookingFee(
            id,
            { EitherT(it.check(UserId("1"), Right(Wallet(it, Amount(2))))) },
            { w -> EitherT(Left(WalletNotFoundError(w.userId)).just()) },
            errorOnInvoke()
        )

        val result = bill(Booking(BookingId("1"), UserId("1"))).value().value()

        assertEquals(Left(WalletNotFoundError(UserId("1"))), result)
    }
}