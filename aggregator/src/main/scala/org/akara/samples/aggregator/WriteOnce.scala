/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013 Akara Sucharitakul
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.akara.samples.aggregator

import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal
import scala.reflect.ClassTag
import scala.collection.generic.CanBuildFrom
import java.util.concurrent.atomic.AtomicInteger

object WriteOnce {
  def apply[T]: WriteOnce[T] = new WriteOnceImpl[T]

  /** Creates an already completed Promise with the specified result.
    *
    *  @tparam T       the type of the value in the promise
    *  @return         the newly created `Promise` object
    */
  def successful[T](result: T): WriteOnce[T] = new impl.Promise.KeptPromise[T](Success(result))


  /** Simple version of `Futures.traverse`. Transforms a `TraversableOnce[Future[A]]` into a `Future[TraversableOnce[A]]`.
    *  Useful for reducing many `Future`s into a single `Future`.
    */
  def sequence[A, M[_] <: TraversableOnce[_]](in: M[WriteOnce[A]])(implicit cbf: CanBuildFrom[M[WriteOnce[A]], A, M[A]]): WriteOnce[M[A]] = {
    in.foldLeft(WriteOnce.successful(cbf(in))) {
      (fr, fa) => for (r <- fr; a <- fa.asInstanceOf[WriteOnce[A]]) yield r += a
    } map (_.result())
  }

  /** Returns a `Future` to the result of the first future in the list that is completed.
    */
  def firstCompletedOf[T](futures: TraversableOnce[WriteOnce[T]]): WriteOnce[T] = {
    val p = WriteOnce[T]

    val completeFirst: Try[T] => Unit = { p.tryComplete(_) }
    futures.foreach(_ onComplete completeFirst)

    p
  }

  /** Returns a `Future` that will hold the optional result of the first `Future` with a result that matches the predicate.
    */
  def find[T](futurestravonce: TraversableOnce[WriteOnce[T]])(predicate: T => Boolean): WriteOnce[Option[T]] = {
    val futures = futurestravonce.toBuffer
    if (futures.isEmpty) WriteOnce.successful[Option[T]](None)
    else {
      val result = WriteOnce[Option[T]]
      val ref = new AtomicInteger(futures.size)
      val search: Try[T] => Unit = v => try {
        v match {
          case Success(r) => if (predicate(r)) result tryComplete Success(Some(r))
          case _ =>
        }
      } finally {
        if (ref.decrementAndGet == 0) {
          result tryComplete Success(None)
        }
      }

      futures.foreach(_ onComplete search)

      result
    }
  }

  /** A non-blocking fold over the specified futures, with the start value of the given zero.
    *  The fold is performed on the thread where the last future is completed,
    *  the result will be the first failure of any of the futures, or any failure in the actual fold,
    *  or the result of the fold.
    *
    *  Example:
    *  {{{
    *    val result = Await.result(Future.fold(futures)(0)(_ + _), 5 seconds)
    *  }}}
    */
  def fold[T, R](futures: TraversableOnce[WriteOnce[T]])(zero: R)(foldFun: (R, T) => R): WriteOnce[R] = {
    if (futures.isEmpty) WriteOnce.successful(zero)
    else sequence(futures).map(_.foldLeft(zero)(foldFun))
  }

  /** Initiates a fold over the supplied futures where the fold-zero is the result value of the `Future` that's completed first.
    *
    *  Example:
    *  {{{
    *    val result = Await.result(Future.reduce(futures)(_ + _), 5 seconds)
    *  }}}
    */
  def reduce[T, R >: T](futures: TraversableOnce[WriteOnce[T]])(op: (R, T) => R): WriteOnce[R] = {
    if (futures.isEmpty) WriteOnce[R].failure(new NoSuchElementException("reduce attempted on empty collection"))
    else sequence(futures).map(_ reduceLeft op)
  }

  /** Transforms a `TraversableOnce[A]` into a `Future[TraversableOnce[B]]` using the provided function `A => Future[B]`.
    *  This is useful for performing a parallel map. For example, to apply a function to all items of a list
    *  in parallel:
    *
    *  {{{
    *    val myFutureList = Future.traverse(myList)(x => Future(myFunc(x)))
    *  }}}
    */
  def traverse[A, B, M[_] <: TraversableOnce[_]](in: M[A])(fn: A => WriteOnce[B])(implicit cbf: CanBuildFrom[M[A], B, M[B]]): WriteOnce[M[B]] =
    in.foldLeft(WriteOnce.successful(cbf(in))) { (fr, a) =>
      val fb = fn(a.asInstanceOf[A])
      for (r <- fr; b <- fb) yield (r += b)
    }.map(_.result)

  private[concurrent] val toBoxed = Map[Class[_], Class[_]](
    classOf[Boolean] -> classOf[java.lang.Boolean],
    classOf[Byte]    -> classOf[java.lang.Byte],
    classOf[Char]    -> classOf[java.lang.Character],
    classOf[Short]   -> classOf[java.lang.Short],
    classOf[Int]     -> classOf[java.lang.Integer],
    classOf[Long]    -> classOf[java.lang.Long],
    classOf[Float]   -> classOf[java.lang.Float],
    classOf[Double]  -> classOf[java.lang.Double],
    classOf[Unit]    -> classOf[scala.runtime.BoxedUnit]
  )
}

trait WriteOnce[+T] {

  def apply() = {
    if (value.isEmpty)
      throw new IllegalStateException("Uninitialized value")
    else value.get
  }

  // TODO!
  def :=(finalValue: T) {
    success(finalValue)
  }

  /** Completes the promise with either an exception or a value.
    *
    *  @param result     Either the value or the exception to complete the promise with.
    *
    *  $promiseCompletion
    */
  def complete(result: Try[T]): this.type =
    if (tryComplete(result)) this else throw new IllegalStateException("Promise already completed.")

  /** Tries to complete the promise with either a value or the exception.
    *
    *  $nonDeterministic
    *
    *  @return    If the promise has already been completed returns `false`, or `true` otherwise.
    */
  def tryComplete(result: Try[T]): Boolean

  /** Completes this promise with the specified future, once that future is completed.
    *
    *  @return   This promise
    */
  final def completeWith(other: WriteOnce[T]): this.type = {
    other onComplete { this.complete }
    this
  }

  /** Attempts to complete this promise with the specified future, once that future is completed.
    *
    *  @return   This promise
    */
  final def tryCompleteWith(other: WriteOnce[T]): this.type = {
    other onComplete { this.tryComplete }
    this
  }

  /** Completes the promise with a value.
    *
    *  @param v    The value to complete the promise with.
    *
    *  $promiseCompletion
    */
  def success(v: T): this.type = complete(Success(v))

  /** Tries to complete the promise with a value.
    *
    *  $nonDeterministic
    *
    *  @return    If the promise has already been completed returns `false`, or `true` otherwise.
    */
  def trySuccess(value: T): Boolean = tryComplete(Success(value))

  /** Completes the promise with an exception.
    *
    *  @param t        The throwable to complete the promise with.
    *
    *  $allowedThrowables
    *
    *  $promiseCompletion
    */
  def failure(t: Throwable): this.type = complete(Failure(t))

  /** Tries to complete the promise with an exception.
    *
    *  $nonDeterministic
    *
    *  @return    If the promise has already been completed returns `false`, or `true` otherwise.
    */
  def tryFailure(t: Throwable): Boolean = tryComplete(Failure(t))


  /* Callbacks */

  /** When this future is completed successfully (i.e. with a value),
    *  apply the provided partial function to the value if the partial function
    *  is defined at that value.
    *
    *  If the future has already been completed with a value,
    *  this will either be applied immediately or be scheduled asynchronously.
    *
    *  $multipleCallbacks
    *  $callbackInContext
    */
  def onSuccess[U](pf: PartialFunction[T, U]): Unit = onComplete {
    case Success(v) if pf isDefinedAt v => pf(v)
    case _ =>
  }

  /** When this future is completed with a failure (i.e. with a throwable),
    *  apply the provided callback to the throwable.
    *
    *  $caughtThrowables
    *
    *  If the future has already been completed with a failure,
    *  this will either be applied immediately or be scheduled asynchronously.
    *
    *  Will not be called in case that the future is completed with a value.
    *
    *  $multipleCallbacks
    *  $callbackInContext
    */
  def onFailure[U](callback: PartialFunction[Throwable, U]): Unit = onComplete {
    case Failure(t) if NonFatal(t) && callback.isDefinedAt(t) => callback(t)
    case _ =>
  }

  /** When this future is completed, either through an exception, or a value,
    *  apply the provided function.
    *
    *  If the future has already been completed,
    *  this will either be applied immediately or be scheduled asynchronously.
    *
    *  $multipleCallbacks
    *  $callbackInContext
    */
  def onComplete[U](func: Try[T] => U): Unit


  /* Miscellaneous */

  /** Returns whether the future has already been completed with
    *  a value or an exception.
    *
    *  $nonDeterministic
    *
    *  @return    `true` if the future is already completed, `false` otherwise
    */
  def isCompleted: Boolean

  /** The value of this `Future`.
    *
    *  If the future is not completed the returned value will be `None`.
    *  If the future is completed the value will be `Some(Success(t))`
    *  if it contains a valid result, or `Some(Failure(error))` if it contains
    *  an exception.
    */
  def value: Option[Try[T]]


  /* Projections */

  /** Returns a failed projection of this future.
    *
    *  The failed projection is a future holding a value of type `Throwable`.
    *
    *  It is completed with a value which is the throwable of the original future
    *  in case the original future is failed.
    *
    *  It is failed with a `NoSuchElementException` if the original future is completed successfully.
    *
    *  Blocking on this future returns a value if the original future is completed with an exception
    *  and throws a corresponding exception if the original future fails.
    */
  def failed: WriteOnce[Throwable] = {
    val p = WriteOnce[Throwable]

    onComplete {
      case Failure(t) => p success t
      case Success(v) => p failure (new NoSuchElementException("Future.failed not completed with a throwable."))
    }
    p
  }


  /* Monadic operations */

  /** Asynchronously processes the value in the future once the value becomes available.
    *
    *  Will not be called if the future fails.
    */
  def foreach[U](f: T => U): Unit = onComplete {
    case Success(r) => f(r)
    case _  => // do nothing
  }

  /** Creates a new future by applying the 's' function to the successful result of
    *  this future, or the 'f' function to the failed result. If there is any non-fatal
    *  exception thrown when 's' or 'f' is applied, that exception will be propagated
    *  to the resulting future.
    *
    *  @param  s  function that transforms a successful result of the receiver into a
    *             successful result of the returned future
    *  @param  f  function that transforms a failure of the receiver into a failure of
    *             the returned future
    *  @return    a future that will be completed with the transformed value
    */
  def transform[S](s: T => S, f: Throwable => Throwable): WriteOnce[S] = {
    val p = WriteOnce[S]

    onComplete {
      case result =>
        try {
          result match {
            case Failure(t)  => p failure f(t)
            case Success(r) => p success s(r)
          }
        } catch {
          case NonFatal(t) => p failure t
        }
    }
    p
  }

  /** Creates a new future by applying a function to the successful result of
    *  this future. If this future is completed with an exception then the new
    *  future will also contain this exception.
    *
    *  $forComprehensionExamples
    */
  def map[S](f: T => S): WriteOnce[S] = { // transform(f, identity)
  val p = WriteOnce[S]

    onComplete {
      case result =>
        try {
          result match {
            case Success(r) => p success f(r)
            case f: Failure[_] => p complete f.asInstanceOf[Failure[S]]
          }
        } catch {
          case NonFatal(t) => p failure t
        }
    }
    p
  }

  /** Creates a new future by applying a function to the successful result of
    *  this future, and returns the result of the function as the new future.
    *  If this future is completed with an exception then the new future will
    *  also contain this exception.
    *
    *  $forComprehensionExamples
    */
  def flatMap[S](f: T => WriteOnce[S]): WriteOnce[S] = {
    val p = WriteOnce[S]

    onComplete {
      case f: Failure[_] => p complete f.asInstanceOf[Failure[S]]
      case Success(v) =>
        try {
          f(v).onComplete({
            case f: Failure[_] => p complete f.asInstanceOf[Failure[S]]
            case Success(v) => p success v
          })
        } catch {
          case NonFatal(t) => p failure t
        }
    }
    p
  }

  /** Creates a new future by filtering the value of the current future with a predicate.
    *
    *  If the current future contains a value which satisfies the predicate, the new future will also hold that value.
    *  Otherwise, the resulting future will fail with a `NoSuchElementException`.
    *
    *  If the current future fails, then the resulting future also fails.
    *
    *  Example:
    *  {{{
    *  val f = future { 5 }
    *  val g = f filter { _ % 2 == 1 }
    *  val h = f filter { _ % 2 == 0 }
    *  Await.result(g, Duration.Zero) // evaluates to 5
    *  Await.result(h, Duration.Zero) // throw a NoSuchElementException
    *  }}}
    */
  def filter(pred: T => Boolean): WriteOnce[T] = {
    val p = WriteOnce[T]

    onComplete {
      case f: Failure[_] => p complete f.asInstanceOf[Failure[T]]
      case Success(v) =>
        try {
          if (pred(v)) p success v
          else p failure new NoSuchElementException("Future.filter predicate is not satisfied")
        } catch {
          case NonFatal(t) => p failure t
        }
    }
    p
  }

  /** Used by for-comprehensions.
    */
  final def withFilter(p: T => Boolean): WriteOnce[T] = filter(p)
  // final def withFilter(p: T => Boolean) = new FutureWithFilter[T](this, p)

  // final class FutureWithFilter[+S](self: Future[S], p: S => Boolean) {
  //   def foreach(f: S => Unit): Unit = self filter p foreach f
  //   def map[R](f: S => R) = self filter p map f
  //   def flatMap[R](f: S => Future[R]) = self filter p flatMap f
  //   def withFilter(q: S => Boolean): FutureWithFilter[S] = new FutureWithFilter[S](self, x => p(x) && q(x))
  // }

  /** Creates a new future by mapping the value of the current future, if the given partial function is defined at that value.
    *
    *  If the current future contains a value for which the partial function is defined, the new future will also hold that value.
    *  Otherwise, the resulting future will fail with a `NoSuchElementException`.
    *
    *  If the current future fails, then the resulting future also fails.
    *
    *  Example:
    *  {{{
    *  val f = future { -5 }
    *  val g = f collect {
    *    case x if x < 0 => -x
    *  }
    *  val h = f collect {
    *    case x if x > 0 => x * 2
    *  }
    *  Await.result(g, Duration.Zero) // evaluates to 5
    *  Await.result(h, Duration.Zero) // throw a NoSuchElementException
    *  }}}
    */
  def collect[S](pf: PartialFunction[T, S]): WriteOnce[S] = {
    val p = WriteOnce[S]

    onComplete {
      case f: Failure[_] => p complete f.asInstanceOf[Failure[S]]
      case Success(v) =>
        try {
          if (pf.isDefinedAt(v)) p success pf(v)
          else p failure new NoSuchElementException("Future.collect partial function is not defined at: " + v)
        } catch {
          case NonFatal(t) => p failure t
        }
    }

    p
  }

  /** Creates a new future that will handle any matching throwable that this
    *  future might contain. If there is no match, or if this future contains
    *  a valid result then the new future will contain the same.
    *
    *  Example:
    *
    *  {{{
    *  future (6 / 0) recover { case e: ArithmeticException => 0 } // result: 0
    *  future (6 / 0) recover { case e: NotFoundException   => 0 } // result: exception
    *  future (6 / 2) recover { case e: ArithmeticException => 0 } // result: 3
    *  }}}
    */
  def recover[U >: T](pf: PartialFunction[Throwable, U]): WriteOnce[U] = {
    val p = WriteOnce[U]

    onComplete { case tr => p.complete(tr recover pf) }

    p
  }

  /** Creates a new future that will handle any matching throwable that this
    *  future might contain by assigning it a value of another future.
    *
    *  If there is no match, or if this future contains
    *  a valid result then the new future will contain the same result.
    *
    *  Example:
    *
    *  {{{
    *  val f = future { Int.MaxValue }
    *  future (6 / 0) recoverWith { case e: ArithmeticException => f } // result: Int.MaxValue
    *  }}}
    */
  def recoverWith[U >: T](pf: PartialFunction[Throwable, WriteOnce[U]]): WriteOnce[U] = {
    val p = WriteOnce[U]

    onComplete {
      case Failure(t) if pf isDefinedAt t =>
        try {
          p completeWith pf(t)
        } catch {
          case NonFatal(t) => p failure t
        }
      case otherwise => p complete otherwise
    }

    p
  }

  /** Zips the values of `this` and `that` future, and creates
    *  a new future holding the tuple of their results.
    *
    *  If `this` future fails, the resulting future is failed
    *  with the throwable stored in `this`.
    *  Otherwise, if `that` future fails, the resulting future is failed
    *  with the throwable stored in `that`.
    */
  def zip[U](that: WriteOnce[U]): WriteOnce[(T, U)] = {
    val p = WriteOnce[(T, U)]

    this onComplete {
      case f: Failure[_] => p complete f.asInstanceOf[Failure[(T, U)]]
      case Success(r) =>
        that onSuccess {
          case r2 => p success ((r, r2))
        }
        that onFailure {
          case f => p failure f
        }
    }

    p
  }

  /** Creates a new future which holds the result of this future if it was completed successfully, or, if not,
    *  the result of the `that` future if `that` is completed successfully.
    *  If both futures are failed, the resulting future holds the throwable object of the first future.
    *
    *  Using this method will not cause concurrent programs to become nondeterministic.
    *
    *  Example:
    *  {{{
    *  val f = future { sys.error("failed") }
    *  val g = future { 5 }
    *  val h = f fallbackTo g
    *  Await.result(h, Duration.Zero) // evaluates to 5
    *  }}}
    */
  def fallbackTo[U >: T](that: WriteOnce[U]): WriteOnce[U] = {
    val p = WriteOnce[U]
    onComplete {
      case s @ Success(_) => p complete s
      case _ => p completeWith that
    }
    p
  }

  /** Creates a new `Future[S]` which is completed with this `Future`'s result if
    *  that conforms to `S`'s erased type or a `ClassCastException` otherwise.
    */
  def mapTo[S](implicit tag: ClassTag[S]): WriteOnce[S] = {
    def boxedType(c: Class[_]): Class[_] = {
      if (c.isPrimitive) WriteOnce.toBoxed(c) else c
    }

    val p = WriteOnce[S]

    onComplete {
      case f: Failure[_] => p complete f.asInstanceOf[Failure[S]]
      case Success(t) =>
        p complete (try {
          Success(boxedType(tag.runtimeClass).cast(t).asInstanceOf[S])
        } catch {
          case e: ClassCastException => Failure(e)
        })
    }

    p
  }

  /** Applies the side-effecting function to the result of this future, and returns
    *  a new future with the result of this future.
    *
    *  This method allows one to enforce that the callbacks are executed in a
    *  specified order.
    *
    *  Note that if one of the chained `andThen` callbacks throws
    *  an exception, that exception is not propagated to the subsequent `andThen`
    *  callbacks. Instead, the subsequent `andThen` callbacks are given the original
    *  value of this future.
    *
    *  The following example prints out `5`:
    *
    *  {{{
    *  val f = future { 5 }
    *  f andThen {
    *    case r => sys.error("runtime exception")
    *  } andThen {
    *    case Failure(t) => println(t)
    *    case Success(v) => println(v)
    *  }
    *  }}}
    */
  def andThen[U](pf: PartialFunction[Try[T], U]): WriteOnce[T] = {
    val p = WriteOnce[T]

    onComplete {
      case r => try if (pf isDefinedAt r) pf(r) finally p complete r
    }

    p
  }

  def onComplete[U](func: Try[T] => U): Unit
}

class WriteOnceImpl[T] extends WriteOnce[T] {
  /** Tries to complete the promise with either a value or the exception.
    *
    * $nonDeterministic
    *
    * @return    If the promise has already been completed returns `false`, or `true` otherwise.
    */
  def tryComplete(result: Try[T]) = ???

  /** Returns whether the future has already been completed with
    * a value or an exception.
    *
    * $nonDeterministic
    *
    * @return    `true` if the future is already completed, `false` otherwise
    */
  def isCompleted = ???

  /** The value of this `Future`.
    *
    * If the future is not completed the returned value will be `None`.
    * If the future is completed the value will be `Some(Success(t))`
    * if it contains a valid result, or `Some(Failure(error))` if it contains
    * an exception.
    */
  def value = ???

  def onComplete[U](func: (Try[T]) => U) {}
}