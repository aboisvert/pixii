package pixii

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import pixii.iterators._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class IteratorsSuite extends WordSpec with ShouldMatchers {
  "PagingIterator" should {
    "initialize with a lazy head" in {
      val service = new MockService(1)
      val iterator = new PagingIterator("test", getNextPage(service), NoRetry)

      service.timesCalled should be (0)
    }

    "be lazy when pulling data" in {
      val service = new MockService(5)
      val iterator = new PagingIterator("test", getNextPage(service), NoRetry)

      iterator.take(2).to[Vector] should be (Vector("x", "x"))
      service.timesCalled should be (2)
    }

    "return all elements when exhausting the underlying data source" in {
      val service = new MockService(5)
      val iterator = new PagingIterator("test", getNextPage(service), NoRetry)

      iterator.to[Vector] should be (Vector("x", "x", "x", "x", "x"))
      service.timesCalled should be (5)
    }
  }

  private def getNextPage(service: MockService) = { (key: Option[Int]) =>
    service.call(key)
  }

  /**
   * A service that emulates a generic paginated api.  Returns paged responses of "x" up to `numPages` pages.  e.g.:
   *
   * ```
   * val service1 = new MockService(1)
   * service1.call(None) // => ("x", None)
   *
   * val service2 = new MockService(2)
   * service2.call(None) // => ("x", Some(1))
   * service2.call(Some(1)) // => ("x", None)
   * ```
   */
  private class MockService(numPages: Int) {
    type PaginationKey = Int
    type Response = String

    var timesCalled = 0

    def call(pageKey: Option[PaginationKey]): (Response, Option[PaginationKey]) = {
      timesCalled += 1
      val key = pageKey.getOrElse(0)

      if (key == numPages - 1) {
        ("x", None)
      } else if (key > numPages - 1) {
        throw new Exception("Exceeded max items!")
      } else {
        ("x", Some(key + 1))
      }
    }
  }
}
