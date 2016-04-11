package pixii

package object iterators {
  /**
   * A generic "paging" iterator that abstracts out DynamoDB's pagination behavior.  When DynamoDB returns a response,
   * it specifies a `LastEvaluatedKey` if the available results are unable to be returned in a single page.  The
   * `LastEvaluatedKey` can then be passed into a subsequent request to continue paging through available results.
   *
   * See docs for more detail:
   * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Pagination
   */
  class PagingIterator[PaginationKey, PagedResponse](
    operationName: String,
    getNextPage: Option[PaginationKey] => (PagedResponse, Option[PaginationKey]),
    retryPolicy: RetryPolicy
  ) extends Iterator[PagedResponse] {
    private var page: Option[PagedResponse] = None
    private var exclusiveStartKey: Option[PaginationKey] = None
    private var morePages = true

    override def hasNext = synchronized {
      if (page.isEmpty && morePages) loadNextPage()

      (exclusiveStartKey.nonEmpty || page.nonEmpty)
    }

    override def next(): PagedResponse = synchronized {
      if (!hasNext) throw new IllegalStateException("hasNext is false")

      val previousPage = page.get
      page = None
      previousPage
    }

    private def loadNextPage() = {
      retryPolicy.retry(operationName) {
        val (nextPage, nextExclusiveStartKey) = getNextPage(exclusiveStartKey)
        if (nextExclusiveStartKey.isEmpty) {
          morePages = false
        }
        exclusiveStartKey = nextExclusiveStartKey
        page = Some(nextPage)
      }
    }
  }
}
