package com.pkaufmann.kotlin.ddd.infrastructure

import arrow.Kind
import arrow.core.*
import arrow.free.*
import arrow.free.extensions.FreeMonad
import arrow.free.extensions.free.monad.monad
import arrow.fx.Resource
import arrow.fx.typeclasses.Async
import arrow.fx.typeclasses.ExitCase
import arrow.fx.typeclasses.ProcF
import arrow.mtl.EitherT
import arrow.mtl.Kleisli
import arrow.mtl.ReaderT
import arrow.mtl.fix
import arrow.typeclasses.Monad
import com.pkaufmann.kotlin.ddd.application.domain.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.statements.InsertStatement
import org.jetbrains.exposed.sql.statements.UpdateStatement
import org.jetbrains.exposed.sql.transactions.transactionManager
import kotlin.coroutines.CoroutineContext

object Wallets : Table() {
    val userId = varchar("userId", 100)
    val amount = integer("amount")

    override val primaryKey = PrimaryKey(userId, name = "PK_Wallets_id")
}

typealias ConnectionIO<A> = Free<ForConnectionOp, A>

fun <G> transK(xa: Transactor<G>): FunctionK<ConnectionIOPartialOf, G> =
    object : FunctionK<ConnectionIOPartialOf, G> {
        override fun <A> invoke(fa: Kind<ConnectionIOPartialOf, A>): Kind<G, A> = fa.fix().trans(xa)
    }

fun <F, A> ConnectionIO<A>.trans(xa: Transactor<F>) =
    xa.trans(this)

fun <D, A, G> Kleisli<D, ConnectionIOPartialOf, A>.trans(xa: Transactor<G>): Kleisli<D, G, A> =
    this.mapK(transK(xa))

fun <D, E, R, G> Kleisli<D, ConnectionIOPartialOf, Either<E, R>>.trans(
    M: Monad<ConnectionIOPartialOf>,
    xa: Transactor<G>
): Kleisli<D, G, Either<E, R>> {
    return this.flatMap<Either<E, R>>(M) {
        when (it) {
            is Either.Left ->
                ReaderT.liftF(Free.liftF(ConnectionOp.Rollback).map { _ -> it })
            is Either.Right ->
                ReaderT.just(M, it)
        }
    }.fix().trans(xa)
}

fun <D, F, A, G> Kleisli<D, F, A>.mapK(f: FunctionK<F, G>): Kleisli<D, G, A> =
    Kleisli { r: D -> f.invoke(this.run(r)) }

infix fun <A, B> (() -> A).andThen(g: (A) -> B): () -> B = { g(this.invoke()) }

infix fun <A, B, C, D> ((A, B) -> C).andThen(g: (C) -> D): (A, B) -> D = { a: A, b: B -> g(this(a, b)) }

interface AsyncConnectionIO<F> : Async<ConnectionIOPartialOf> {
    val free: FreeMonad<ForConnectionOp>
        get() = Free.monad()

    override fun <A> asyncF(k: ProcF<ConnectionIOPartialOf, A>): Kind<ConnectionIOPartialOf, A> {
        return Free.liftF(ConnectionOp.AsyncF(k))
    }

    override fun <A> defer(fa: () -> Kind<ConnectionIOPartialOf, A>): Kind<ConnectionIOPartialOf, A> {
        return Free.liftF(ConnectionOp.Delay(fa.andThen { it.fix() }))
    }

    override fun <A> just(a: A): Kind<ConnectionIOPartialOf, A> {
        return Free.liftF(ConnectionOp.JustOp(a))
    }

    override fun <A> raiseError(e: Throwable): Kind<ConnectionIOPartialOf, A> =
        Free.liftF(ConnectionOp.RaiseErrorOp(e))

    override fun <A, B> tailRecM(
        a: A,
        f: (A) -> Kind<ConnectionIOPartialOf, Either<A, B>>
    ): Kind<ConnectionIOPartialOf, B> =
        free.tailRecM(a, f)

    override fun <A, B> Kind<ConnectionIOPartialOf, A>.bracketCase(
        release: (A, ExitCase<Throwable>) -> Kind<ConnectionIOPartialOf, Unit>,
        use: (A) -> Kind<ConnectionIOPartialOf, B>
    ): Kind<ConnectionIOPartialOf, B> = Free.liftF(
        ConnectionOp.BracketCase(
            fix(),
            use.andThen { it.fix() },
            release.andThen { it.fix() }
        )
    )

    override fun <A> Kind<ConnectionIOPartialOf, A>.continueOn(ctx: CoroutineContext): Kind<ConnectionIOPartialOf, A> =
        Free.liftF(ConnectionOp.EvalOn(ctx, this.fix()))

    override fun <A, B> Kind<ConnectionIOPartialOf, A>.flatMap(f: (A) -> Kind<ConnectionIOPartialOf, B>): Kind<ConnectionIOPartialOf, B> =
        free.fx.monad {
            val r = !this@flatMap
            !f(r)
        }

    override fun <A> Kind<ConnectionIOPartialOf, A>.handleErrorWith(f: (Throwable) -> Kind<ConnectionIOPartialOf, A>): Kind<ConnectionIOPartialOf, A> {
        return Free.liftF(ConnectionOp.HandleErrorWith(this.fix(), f.andThen { it.fix() }))
    }
}

fun <F> asyncConnectionIO(): AsyncConnectionIO<F> =
    object : AsyncConnectionIO<F> {}

typealias ConnectionIOPartialOf = FreePartialOf<ForConnectionOp>

sealed class ConnectionOp<out A> : ConnectionOpOf<A> {
    data class AsyncF<A>(val p: ProcF<ConnectionIOPartialOf, A>) : ConnectionOp<A>()

    data class Delay<A>(val p: () -> ConnectionIO<A>) : ConnectionOp<A>()

    data class Async<A>(val r: (Transaction) -> A) : ConnectionOp<A>()

    data class JustOp<A>(val v: A) : ConnectionOp<A>()

    data class RaiseErrorOp<A>(val e: Throwable) : ConnectionOp<A>()

    data class HandleErrorWith<A>(val fa: ConnectionIO<A>, val f: (Throwable) -> ConnectionIO<A>) : ConnectionOp<A>()

    data class BracketCase<A, B>(
        val aquire: ConnectionIO<A>,
        val use: (A) -> ConnectionIO<B>,
        val release: (A, ExitCase<Throwable>) -> ConnectionIO<Unit>
    ) : ConnectionOp<B>() {
        fun <F> invoke(A: arrow.fx.typeclasses.Async<F>, outer: FunctionK<ForConnectionOp, F>): Kind<F, B> {
            return A.fx.async {
                !aquire.foldMap(outer, A).bracketCase(
                    { a, e -> release(a, e).foldMap(outer, A) },
                    { r -> use(r).foldMap(outer, A) }
                )
            }
        }
    }

    data class EvalOn<A>(val ctx: CoroutineContext, val kind: ConnectionIO<A>) : ConnectionOp<A>()

    object Commit : ConnectionOp<Unit>()

    object Rollback : ConnectionOp<Unit>()

    companion object : FreeMonad<ForConnectionOp> {}
}

fun Query.query(): Free<ForConnectionOp, List<ResultRow>> =
    Free.liftF(ConnectionOp.Async {
        it.exec(this) { r ->
            val rows = mutableListOf<ResultRow>()
            while (r.next()) {
                rows.add(ResultRow.create(r, this@query.set.fields))
            }
            rows.toList()
        }.orEmpty()
    })

fun <A : Any> InsertStatement<A>.run(): Free<ForConnectionOp, Int?> = Free.liftF(ConnectionOp.Async { tx ->
    tx.exec(this)
})

fun UpdateStatement.run(): Free<ForConnectionOp, Int?> = Free.liftF(ConnectionOp.Async { tx ->
    tx.exec(this)
})

class Transactor<F>(private val db: Database, private val AF: Async<F>) {
    fun <A> trans(con: ConnectionIO<A>): Kind<F, A> = //transP().invoke(con)
        Resource.invoke(
            { AF.effect { db.transactionManager.newTransaction() } },
            { tx: Transaction -> AF.effect { tx.close() } },
            AF
        ).use { tx: Transaction ->
            val r = ConnectionOp.fx.monad { !con }.fix().foldMap(ioInterpreter(AF, tx), AF)
            AF.fx.async {
                !r
                    .flatMap { r -> AF.effect { tx.commit(); r } }
                    .handleErrorWith { t -> AF.effect { tx.rollback(); throw t } }
            }
        }
}

fun <F> ioInterpreter(A: Async<F>, tx: Transaction): FunctionK<ForConnectionOp, F> =
    object : FunctionK<ForConnectionOp, F> {
        override fun <A> invoke(fa: Kind<ForConnectionOp, A>): Kind<F, A> {
            val op = fa.fix()
            return when (op) {
                is ConnectionOp.Async<*> ->
                    A.effect { op.r(tx) }
                is ConnectionOp.AsyncF<*> ->
                    A.asyncF(op.p.andThen { it.fix().foldMap(ioInterpreter(A, tx), A) })
                is ConnectionOp.Delay<*> ->
                    A.defer { op.p().fix().foldMap(ioInterpreter(A, tx), A) }
                is ConnectionOp.JustOp<*> ->
                    A.just(op.v)
                is ConnectionOp.Commit ->
                    A.effect { tx.commit() }
                is ConnectionOp.Rollback ->
                    A.effect { tx.rollback() }
                is ConnectionOp.RaiseErrorOp<*> ->
                    A.raiseError(op.e)
                is ConnectionOp.HandleErrorWith<*> -> {
                    val outer = this
                    A.fx.async {
                        !op.fa.foldMap(outer, A)
                            .handleErrorWith(op.f.andThen { it.foldMap(outer, A) })
                    }
                }
                is ConnectionOp.BracketCase<*, *> ->
                    op.invoke(A, this)
                is ConnectionOp.EvalOn<*> -> {
                    val outer = this
                    A.fx.async {
                        this.continueOn(op.ctx)
                        !op.kind.foldMap(outer, A)
                    }
                }
            } as Kind<F, A>
        }
    }

class ForConnectionOp private constructor() {
    companion object
}
typealias ConnectionOpOf<A> = Kind<ForConnectionOp, A>

inline fun <A> ConnectionOpOf<A>.fix(): ConnectionOp<A> = this as ConnectionOp<A>

object JdbcWalletRepository {

    fun insert(): Insert<ConnectionIOPartialOf> =
        { wallet ->
            val stmt = InsertStatement<Number>(Wallets)
            stmt[Wallets.userId] = wallet.userId.id
            stmt[Wallets.amount] = wallet.amount.amount
            stmt.run().map { Unit }
        }

    fun get(): Get<ConnectionIOPartialOf> =
        { userId ->
            EitherT(Wallets.select { Wallets.userId.eq(userId.id) }
                .query()
                .map {
                    it.firstOrNull()
                        .rightIfNotNull { WalletNotFoundError(userId) }
                        .map { Wallet(UserId(it[Wallets.userId]), Amount(it[Wallets.amount])) }
                })
        }

    fun update(): Update<ConnectionIOPartialOf> =
        { wallet ->
            val stmt = UpdateStatement(Wallets, 1, Wallets.userId.eq(wallet.userId.id))
            stmt[Wallets.amount] = wallet.amount.amount

            EitherT(stmt.run()
                .map {
                    it
                        .rightIfNotNull { WalletNotFoundError(wallet.userId) }
                        .flatMap { if (it > 0) Right(Unit) else Left(WalletNotFoundError(wallet.userId)) }
                })
        }


}