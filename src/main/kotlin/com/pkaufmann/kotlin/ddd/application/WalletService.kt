package com.pkaufmann.kotlin.ddd.application

import arrow.mtl.EitherT
import arrow.mtl.extensions.eithert.monad.monad
import arrow.mtl.extensions.eithert.monadTrans.liftT
import arrow.mtl.fix
import arrow.typeclasses.Monad
import com.pkaufmann.kotlin.ddd.application.domain.*

typealias BillBookingFee<F> = (Booking) -> EitherT<BillBookingFeeError, F, Amount>

object WalletService {
    fun <F> billBookingFee(
        MF: Monad<F>,
        getWallet: Get<F>,
        updateWallet: Update<F>,
        send: Send<BookingBilledMessage, F>
    ): BillBookingFee<F> =
        { booking ->
            EitherT.monad<BillBookingFeeError, F>(MF).fx.monad {
                val wallet = !getWallet(booking.userId)
                val billedWallet = wallet.billBookingFee(booking)

                !updateWallet(billedWallet)

                !send(BookingBilledMessage(booking.bookingId, billedWallet.amount))
                    .liftT<WalletNotFoundError, F, Unit>(MF)

                billedWallet.amount
            }.fix()
        }
}