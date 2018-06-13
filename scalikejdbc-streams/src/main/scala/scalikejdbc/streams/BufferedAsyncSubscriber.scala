package scalikejdbc.streams

import java.util
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import java.util.concurrent.{ BlockingQueue, LinkedBlockingQueue }

import org.reactivestreams.{ Subscriber, Subscription }
import scalikejdbc.LogSupport
import scalikejdbc.streams.commands.ConsumingCommand

import scala.collection.JavaConverters._
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

/**
 * AsyncSubscriber for the DatabasePublisher's convenient methods.
 *
 * @param batchSize     buffer size and it equals request size.
 * @param asyncExecutor async executor.
 * @param initial       initial state data.
 * @param handle        handle to consume and fold the streaming data.
 * @tparam A consuming data type.
 * @tparam R returning data type.
 */
private[streams] class BufferedAsyncSubscriber[A, R](
  batchSize: Int,
  asyncExecutor: AsyncExecutor)(initial: R)(handle: (A, R) => ConsumingCommand[R])
  extends Subscriber[A] with LogSupport {

  private[this] final val maybeSubscription: AtomicReference[Option[Subscription]] = new AtomicReference[Option[Subscription]](None)

  private[this] final val result: AtomicReference[R] = new AtomicReference[R](initial)
  private[this] final val buffer: BlockingQueue[A] = new LinkedBlockingQueue[A](batchSize)

  private[this] final val _isWorkerAlreadyStarted: AtomicBoolean = new AtomicBoolean(false)
  private[this] final val scheduledElements: BlockingQueue[Seq[A]] = new LinkedBlockingQueue[Seq[A]]()

  /**
   * Whether the Subscriber has been signaled with `onComplete` or `onError`.
   */
  private[this] final val _isCurrentSubscriberFinished: AtomicBoolean = new AtomicBoolean(false)
  private def isCurrentSubscriberFinished: Boolean = _isCurrentSubscriberFinished.get()

  /**
   * Returns true if it has been cancelled by the Subscriber.
   */
  private[this] final val _isCancellationAlreadyRequested: AtomicBoolean = new AtomicBoolean(false)
  private def isCancellationAlreadyRequested: Boolean = _isCancellationAlreadyRequested.get()

  private[this] final val endOfStream: Promise[R] = Promise[R]()

  /**
   * Provides consumed result data.
   */
  def future: Future[R] = endOfStream.future

  // -----------------------------------------------
  // Reactive Streams Subscriber APIs
  // -----------------------------------------------

  override def onSubscribe(s: Subscription): Unit = {
    if (s == null) {
      // 2-13: when any provided parameter is null in which case it MUST throw a java.lang.NullPointerException to the caller
      throw new NullPointerException("given Subscription to AsyncSubscriber#onSubscribe is null. (Reactive Streams spec, 2.13)")

    } else if (maybeSubscription.compareAndSet(None, Some(s))) {
      doRequest(s)

    } else {
      // 2-5: A Subscriber MUST call Subscription.cancel() on the given Subscription after an onSubscribe signal if it already has an active Subscription.
      doCancel(s)

    }
  }

  override def onNext(t: A): Unit = {
    if (t == null) {
      // 2-13: when any provided parameter is null in which case it MUST throw a java.lang.NullPointerException to the caller
      throw new NullPointerException("given element to AsyncSubscriber#onNext is null. (Reactive Streams spec, 2.13)")
    }

    if (!isCurrentSubscriberFinished && !isCancellationAlreadyRequested) {
      try {
        buffer.put(t)

        if (buffer.size() >= batchSize) {
          val consumingRecords = new util.ArrayList[A](batchSize)
          buffer.drainTo(consumingRecords, batchSize)
          scheduleSynchronousStreaming(IndexedSeq(consumingRecords.asScala: _*))
        }

      } catch { // TODO: should cancel?
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
  }

  override def onError(t: Throwable): Unit = {
    if (t == null) {
      // 2-13: when any provided parameter is null in which case it MUST throw a java.lang.NullPointerException to the caller
      throw new NullPointerException("given throwable object to AsyncSubscriber#onError is null. (Reactive Streams spec, 2.13)")
    }

    if (!_isCurrentSubscriberFinished.getAndSet(true)) {
      if (log.isDebugEnabled) {
        log.debug(s"Subscriber#onError: ${this} called with exception: $t")
      }

      maybeSubscription.set(None)
      endOfStream.tryFailure(t)
    }
  }

  override def onComplete(): Unit = {
    if (!_isCurrentSubscriberFinished.getAndSet(true)) {
      if (log.isDebugEnabled) {
        log.debug(s"Subscriber#onComplete: ${this} called")
      }

      maybeSubscription.set(None)
      endOfStream.trySuccess(result.get())
    }
  }

  // -----------------------------------------------
  // Internal APIs
  // -----------------------------------------------

  private[this] def doRequest(s: Subscription): Unit = {
    if (!isCurrentSubscriberFinished && !isCancellationAlreadyRequested) {
      try {
        s.request(batchSize)
      } catch {
        case NonFatal(e) =>
          log.warn(s"Subscription#request: $s unexpectedly failed because ${e.getMessage}. (Reactive Streams spec, 3.16)", e)
      }
    }
  }

  private[this] def doCancel(s: Subscription): Unit = {
    if (!_isCancellationAlreadyRequested.getAndSet(true)) {
      try {
        s.cancel()
      } catch {
        case NonFatal(e) =>
          log.warn(s"Subscription#cancel: $s unexpectedly failed because ${e.getMessage}. (Reactive Streams spec, 3.15)", e)
      }
    }
  }

  private def requestNextElements(): Unit = {
    if (!isCurrentSubscriberFinished) {
      maybeSubscription.get() match {
        case Some(subscription) =>
          doRequest(subscription)
        case _ =>
      }
    }
  }

  private def cancelSubscribing(): Unit = {
    if (!isCurrentSubscriberFinished) {
      maybeSubscription.get() match {
        case Some(subscription) =>
          doCancel(subscription)
        case _ =>
      }
    }
  }

  private def updateResult(newResult: R): Unit = {
    if (!isCurrentSubscriberFinished) {
      result.set(newResult)
    }
  }

  private[this] def scheduleSynchronousStreaming(records: IndexedSeq[A]): Unit = {
    scheduledElements.add(records)
    if (_isWorkerAlreadyStarted.compareAndSet(false, true)) {
      startWorker()
    }
    requestNextElements()
  }

  private def startWorker(): Unit = {
    val currentSubscriber = this

    try {
      val task: Runnable = new Runnable {
        override def run(): Unit = {
          try {
            var newResult: R = result.get()
            while (!scheduledElements.isEmpty) {
              val records = scheduledElements.take()
              var i = 0
              while (i < records.length && !isCurrentSubscriberFinished && !isCancellationAlreadyRequested) {
                val nextValue = records(i)
                newResult = {
                  handle(nextValue, newResult) match {
                    case ConsumingCommand.Continue(newValue) =>
                      newValue
                    case ConsumingCommand.Break(newValue) =>
                      currentSubscriber.cancelSubscribing()
                      newValue
                  }
                }
                i += 1
              }
            }

            currentSubscriber.updateResult(newResult)
            _isWorkerAlreadyStarted.set(false)

          } catch {
            case NonFatal(e) =>
              if (log.isDebugEnabled) {
                log.debug(s"Unexpectedly failed to deal with consuming buffer because ${e.getMessage}", e)
              } else {
                log.info(s"Unexpectedly failed to deal with consuming buffer because ${e.getMessage}, exception: ${e.getClass.getCanonicalName}")
              }
              // TODO: re-throw ?
              throw e

          }
        }
      }

      asyncExecutor.execute(task)

    } catch {
      case NonFatal(e) =>
        // If we can't run on the `Executor`, we need to fail gracefully and not violate rule 2.13
        log.warn(s"Failed to schedule a synchronous processing because ${e.getMessage}", e)
        cancelSubscribing()
    }
  }
}
