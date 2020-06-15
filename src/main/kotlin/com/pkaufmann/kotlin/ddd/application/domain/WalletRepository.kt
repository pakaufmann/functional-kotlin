package com.pkaufmann.kotlin.ddd.application.domain

import arrow.Kind
import arrow.core.Either
import arrow.core.Option
import arrow.mtl.EitherT

typealias Insert<F> = (Wallet) -> Kind<F, Unit>

typealias Get<F> = (UserId) -> EitherT<WalletNotFoundError, F, Wallet>

typealias Update<F> = (Wallet) -> EitherT<WalletNotFoundError, F, Unit>