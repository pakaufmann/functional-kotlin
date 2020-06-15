import arrow.Kind
import arrow.core.andThen
import arrow.free.Free
import arrow.free.extensions.free.monad.monad
import arrow.free.fix
import arrow.fx.IO
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.io.unsafeRun.unsafeRun
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.UnsafeRun
import arrow.mtl.*
import arrow.mtl.extensions.eithert.monadThrow.monadThrow
import arrow.mtl.extensions.kleisli.monad.monad
import arrow.typeclasses.MonadThrow
import arrow.unsafe
import com.pkaufmann.kotlin.ddd.application.WalletService
import com.pkaufmann.kotlin.ddd.application.domain.*
import com.pkaufmann.kotlin.ddd.infrastructure.*
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.util.*
import kotlin.random.Random

fun <F> UnsafeRun<F>.main(fx: Concurrent<F>) {
    val db = Database.connect(
        url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
        driver = "org.h2.Driver",
        user = "root",
        password = ""
    )

    val xa = Transactor(db, fx)

    val ctxFrMT = Reader.monad<Context, ConnectionIOPartialOf>(Free.monad())
    val aConAF = asyncConnectionIO<F>()

    val updateWithRetry = JdbcWalletRepository.update()
        .metric()
        .chaos(aConAF, 0.5f)
        .retry(EitherT.monadThrow<WalletNotFoundError, ConnectionIOPartialOf>(aConAF), 3)
        .andThen { it.fix() }

    val bill = WalletService.billBookingFee(
        ctxFrMT,
        JdbcWalletRepository.get().andThen { it.liftReader() },
        updateWithRetry.andThen { it.liftReader() },
        InMemoryMessageSender.sendWithContext(aConAF)
    ).andThen { it.value().fix().trans(aConAF, xa) }

    transaction(db) { SchemaUtils.create(Wallets) }

    val billBooking = bill(Booking(BookingId("1"), UserId("2")))

    val insert = JdbcWalletRepository.insert()
        .invoke(Wallet(UserId("2"), Amount(3)))
        .fix()
        .trans(xa)

    unsafe {
        runBlocking {
            fx.bindingConcurrent {
                !insert

                val first = !billBooking
                    .run(Context(UUID.randomUUID()))
                    .handleError { _ -> println("got an error in 1") }

                val second = !billBooking
                    .run(Context(UUID.randomUUID()))
                    .handleError { _ -> println("Got an error in 2") }

                val third = !billBooking
                    .run(Context(UUID.randomUUID()))
                    .handleError { _ -> println("Got an error in 3") }

                println(first)
                println(second)
                println(third)
            }
        }
    }
}

fun <L, F, R> EitherT<L, Kind<Kind<ForKleisli, Transaction>, F>, R>.liftTxR(): EitherT<L, Kind<Kind<ForKleisli, Transaction>, Kind<Kind<ForKleisli, Context>, F>>, R> =
    EitherT(ReaderT { tx: Transaction -> ReaderT { _: Context -> this.value().run(tx) } })

fun <L, F, R> EitherT<L, F, R>.liftTx(): EitherT<L, Kind<Kind<ForKleisli, Transaction>, F>, R> =
    EitherT(ReaderT.liftF(this.value()))

fun <L, F, R> EitherT<L, F, R>.liftReader(): EitherT<L, Kind<Kind<ForKleisli, Context>, F>, R> =
    EitherT(ReaderT.liftF(this.value()))

class ChaosException : Throwable()

fun <T, L, F, R> ((T) -> EitherT<L, F, R>).chaos(MF: MonadThrow<F>, chance: Float): (T) -> EitherT<L, F, R> =
    { i ->
        if (Random.nextFloat() < chance) {
            EitherT(MF.raiseError(ChaosException()))
        } else {
            this.invoke(i)
        }
    }


fun <T, F, R> ((T) -> Kind<F, R>).retry(MF: MonadThrow<F>, times: Int): (T) -> Kind<F, R> =
    { i ->
        MF.fx.monadThrow {
            val f = invoke(i)
            !f.handleErrorWith { e ->
                if (times <= 0) {
                    println("Got error, out of retries, aborting")
                    raiseError(e)
                } else {
                    println("Got error, retrying again: times left=${times}")
                    retry(MF, times - 1).invoke(i)
                }
            }
        }
    }

fun <T, L, F, R> ((T) -> EitherT<L, F, R>).metric(): (T) -> EitherT<L, F, R> =
    { i ->
        val start = System.nanoTime()
        val result = invoke(i)
        println("Took time: ${(System.nanoTime() - start) / 1000000.0} ms")
        result
    }

fun main(args: Array<String>) {
    IO.unsafeRun().main(IO.concurrent())
}