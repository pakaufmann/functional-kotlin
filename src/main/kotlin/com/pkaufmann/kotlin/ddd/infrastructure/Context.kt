package com.pkaufmann.kotlin.ddd.infrastructure

import arrow.mtl.ReaderTPartialOf
import java.util.*

typealias Ctx<F> = ReaderTPartialOf<Context, F>

data class Context(val contextId: UUID)