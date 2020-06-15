package com.pkaufmann.kotlin.ddd.infrastructure

import arrow.core.Either
import arrow.core.Left
import arrow.core.Right
import arrow.fx.typeclasses.Async
import arrow.mtl.EitherT
import arrow.mtl.ReaderT
import arrow.mtl.ReaderTPartialOf
import com.pkaufmann.kotlin.ddd.application.domain.*
import kotlin.random.Random

object InMemoryWalletRepository {
    private val wallets = HashMap<UserId, Wallet>()

    fun <F> insert(A: Async<F>): Insert<F> =
        { wallet ->
            A.async {
                wallets[wallet.userId] = wallet
            }
        }

    fun <F> get(A: Async<F>): Get<F> =
        { userId ->
            EitherT(A.async {
                val wallet = wallets[userId]

                if (wallet != null) {
                    Right(wallet)
                } else {
                    Left(WalletNotFoundError(userId))
                }
            })
        }

    fun <F> update(A: Async<F>): Update<F> =
        { wallet ->
            EitherT(A.async {
                if (!wallets.containsKey(wallet.userId)) {
                    Left(WalletNotFoundError(wallet.userId))
                } else {
                    Either.Right(wallets.set(wallet.userId, wallet))
                }
            })
        }
}