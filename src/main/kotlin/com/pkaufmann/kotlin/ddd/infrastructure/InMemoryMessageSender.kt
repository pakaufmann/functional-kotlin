package com.pkaufmann.kotlin.ddd.infrastructure

import arrow.fx.typeclasses.Async
import arrow.mtl.ReaderT
import arrow.mtl.ReaderTPartialOf
import com.pkaufmann.kotlin.ddd.application.domain.Send

object InMemoryMessageSender {
    fun <F, T> sendWithContext(AF: Async<F>): Send<T, ReaderTPartialOf<Context, F>> =
        { message ->
            ReaderT.invoke { context ->
                AF.effect {
                    println("Sending message in context: $message, context=$context")
                }
            }
        }

    fun <F, T> send(AF: Async<F>): Send<T, F> =
        { message ->
            AF.effect {
                println("Sending message in context: $message")
            }
        }
}