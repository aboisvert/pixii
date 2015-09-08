package pixii

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodbv2.model._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/** Plugagable retry policy */
trait RetryPolicy {
  def retry[T](operation: String)(f: => T): T
}

object RetryPolicy {
  class StopRetryingException(cause: Exception) extends RuntimeException(cause)

  class RetrySilentlyException extends RuntimeException

  def dontRetryClientError[T](f: => T): T = {
    try {
      f
    } catch {
      case e: AmazonServiceException if e.getErrorType == AmazonServiceException.ErrorType.Client
                                        && !e.isInstanceOf[ProvisionedThroughputExceededException] =>
        // no use retrying if the error is a client-side probllem
        throw new StopRetryingException(e)

      case e: ValidationException =>
        throw new StopRetryingException(e)
    }
  }
}

object NoRetry extends RetryPolicy {
  override def retry[T](operation: String)(f: => T): T = f
}

class FibonacciBackoff(
  val retryDuration: (Long, TimeUnit),
  val log: Logger
) extends RetryPolicy {
  import RetryPolicy._

  def this(retryDuration: Duration, log: Logger) = {
    this((retryDuration.length, retryDuration.unit), log)
  }

  def retry[T](operation: String)(f: => T): T = {
    val start = System.currentTimeMillis()
    val limitMillis = retryDuration._2.toMillis(retryDuration._1)
    var backoff = 100L
    var result: T = null.asInstanceOf[T]
    var lastException: Exception = null

    def elapsed = System.currentTimeMillis() - start
    do {
      try {
        return RetryPolicy.dontRetryClientError(f)
      } catch {
        case e: StopRetryingException =>
          log.error("Operation %s interrupted." format operation, e.getCause)
          if (e.getCause != null) throw e.getCause else throw e

        case e: Exception =>
          // fall through and retry
          // log.error("Exception during %s:\n%s: %s" format (operation, e.getClass.getName, Option(e.getMessage) getOrElse ""), e)
          lastException = e
          if (!e.isInstanceOf[RetrySilentlyException]) {
            log.warn("Exception during %s:\n%s: %s" format (operation, e.getClass.getName, Option(e.getMessage) getOrElse ""))
            log.warn("Sleeping " + backoff + "ms ...")
          }
          Thread.sleep(backoff)
          // note:  8 / 5 is used as approximation for golden ratio (the 6th and 5th fibonacci numbers, respectively)
          backoff = math.min(backoff * 8 / 5, 10 * 60 * 1000L) // limit backoff to 10 mins
      }
    } while (elapsed < limitMillis)
    log.error("Too many exception during %s; aborting" format operation, lastException)
    throw lastException
  }
}
