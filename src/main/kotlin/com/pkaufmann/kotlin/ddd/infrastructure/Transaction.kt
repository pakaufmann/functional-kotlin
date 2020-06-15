package com.pkaufmann.kotlin.ddd.infrastructure

import arrow.mtl.ReaderTPartialOf
import org.jetbrains.exposed.sql.Transaction

typealias Tx<F> = ReaderTPartialOf<Transaction, F>