package pixii

import pixii.KeySchema._

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.services.dynamodb._
import com.amazonaws.services.dynamodb.model._

import scala.collection._
import scala.collection.JavaConversions._

trait Table[T] {
  /** User-provided table name */
  val tableName: String

  /** User-provided retry policy */
  val retryPolicy: RetryPolicy

  /** User-provided DynamoDB client */
  val dynamoDB: AmazonDynamoDB

  /** User-provided item mapper */
  val itemMapper: ItemMapper[T]
}

trait TableOperations[K,  V] { self: Table[V] =>
  /** Table's key schema */
  val schema: KeySchema[K]

  /** Convert key value into a dynamodb.model.Key */
  def toKey(k: K): Key

  /** Retrieve value associated with given key */
  def apply(key: K)(implicit consistency: Consistency): Option[V] = {
    retryPolicy.retry("Table(%s).apply(%s)" format (tableName, key)) {
      try {
        val request = (new GetItemRequest()
          .withTableName(tableName)
          .withKey(toKey(key))
          .withConsistentRead(consistency.isConsistentRead)
        )
        val response = dynamoDB.getItem(request)
        Option(response.getItem) map { item => itemMapper.unapply(item.toMap) }
      } catch { case e: ResourceNotFoundException =>
        return None
      }
    }
  }

  /** Put value into table */
  def delete(key: K): Unit = {
    retryPolicy.retry("Table(%s).delete(%s)" format (tableName, key)) {
      val request = (new DeleteItemRequest()
        .withTableName(tableName)
          .withKey(toKey(key))
      )
      val response = dynamoDB.deleteItem(request)
    }
  }

  /** Put value into table */
  def put(value: V): Unit = {
    retryPolicy.retry("Table(%s).put(%s)" format (tableName, value)) {
      val request = (new PutItemRequest()
        .withTableName(tableName)
        .withItem(itemMapper.apply(value))
      )
      val response = dynamoDB.putItem(request)
    }
  }

  /** Update an item */
  def update(key: K, attributeUpdates: Map[String, AttributeValueUpdate]): Unit = {
    retryPolicy.retry("Table(%s).update(%s)" format (tableName, key)) {
      val request = (new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(toKey(key))
        .withAttributeUpdates(attributeUpdates))
      dynamoDB.updateItem(request)
    }
  }

  // TODO:  queryCount()
  // TODO:  scanCount()

  def scan(
    filter: Map[String, Condition] = Map.empty,
    evaluateItemPageLimit: Int = -1
  ): Iterator[V] = {
    val request = new ScanRequest().withTableName(tableName)

    if (filter.nonEmpty) request.setScanFilter(filter)

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[Key]) = {
      if (exclusiveStartKey.isDefined) request.setExclusiveStartKey(exclusiveStartKey.get)
      val response = dynamoDB.scan(request)
      (response.getItems, response.getLastEvaluatedKey)
    }

    new Iterator[V] {
      private val iter = iterator("scanSelectedAttributes", nextPage)
      override def hasNext = iter.hasNext
      override def next: V = itemMapper.unapply(iter.next)
    }
  }

  def scanSelectedAttributes[T](
    attributesToGet: Seq[String],
    itemMapper: Map[String, AttributeValue] => T,
    filter: Map[String, Condition] = Map.empty,
    evaluateItemPageLimit: Int = -1
  ): Iterator[T] = {
    val request = (new ScanRequest()
      .withTableName(tableName)
      .withAttributesToGet(attributesToGet))

    if (filter.nonEmpty) request.setScanFilter(filter)

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[Key]) = {
      if (exclusiveStartKey.isDefined) request.setExclusiveStartKey(exclusiveStartKey.get)
      val response = dynamoDB.scan(request)
      (response.getItems, response.getLastEvaluatedKey)
    }

    new Iterator[T] {
      private val iter = iterator("scanSelectedAttributes", nextPage)
      override def hasNext = iter.hasNext
      override def next = itemMapper.apply(iter.next)
    }
  }

  protected def iterator(
    operationName: String,
    nextPage: Option[Key] => (java.util.List[java.util.Map[String, AttributeValue]], Key)
  ): Iterator[Map[String, AttributeValue]] = new Iterator[Map[String, AttributeValue]] {
    private var index = 0
    private var items: java.util.List[java.util.Map[String, AttributeValue]] = null
    private var morePages = true
    private var exclusiveStartKey: Option[Key] = None

    override def hasNext = synchronized {
      if ((items == null || index >= items.size) && morePages) loadNextPage()
      (items != null && index < items.size)
    }

    override def next(): Map[String, AttributeValue] = synchronized {
      if (!hasNext) throw new IllegalStateException("hasNext is false")
      val item = items.get(index)
      index += 1
      item
    }

    private def loadNextPage() = {
      retryPolicy.retry(operationName) {
        val (nextItems, nextExclusiveStartKey) = nextPage(exclusiveStartKey)
        if (nextExclusiveStartKey == null) {
          morePages = false
        }
        exclusiveStartKey = Some(nextExclusiveStartKey)
        items = nextItems
        index = 0
      }
    }
  }


  def createTable(readCapacity: Long, writeCapacity: Long): Unit = {
    val provisionedThroughput = (new ProvisionedThroughput()
      .withReadCapacityUnits(readCapacity)
      .withWriteCapacityUnits(writeCapacity)
    )
    val request = (new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(schema.keySchema)
      .withProvisionedThroughput(provisionedThroughput)
    )
    dynamoDB.createTable(request)
  }

  def deleteTable_!(): Unit = {
    val deleteTableRequest = new DeleteTableRequest().withTableName(tableName)
    dynamoDB.deleteTable(deleteTableRequest)
  }
}

trait HashKeyTable[H, V] extends TableOperations[H, V] { self: Table[V] =>
  val hashKeyAttribute: NamedAttribute[H]

  override lazy val schema = HashKeySchema[H](hashKeyAttribute)
  override def toKey(key: H): Key = new Key().withHashKeyElement(hashKeyAttribute(key).valuesIterator.next())
}

trait HashAndRangeKeyTable[H, R, V] extends TableOperations[(H, R), V] { self: Table[V] =>
  val hashKeyAttribute: NamedAttribute[H]
  val rangeKeyAttribute: NamedAttribute[R]

  override lazy val schema = HashAndRangeKeySchema[H, R](hashKeyAttribute, rangeKeyAttribute)
  override def toKey(key: (H, R)): Key = (new Key()
    .withHashKeyElement(hashKeyAttribute(key._1).valuesIterator.next())
    .withRangeKeyElement(rangeKeyAttribute(key._2).valuesIterator.next()))

  def query(
    hashKeyValue: H,
    rangeKeyCondition: Condition = null,
    evaluateItemPageLimit: Int = -1
  )(implicit consistency: Consistency): Iterator[V] = {
    val request = (new QueryRequest()
      .withTableName(tableName)
      .withHashKeyValue(hashKeyAttribute(hashKeyValue).valuesIterator.next())
      .withConsistentRead(consistency.isConsistentRead))

    if (rangeKeyCondition != null) request.setRangeKeyCondition(rangeKeyCondition)

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[Key]) = {
      if (exclusiveStartKey.isDefined) request.setExclusiveStartKey(exclusiveStartKey.get)
      val response = dynamoDB.query(request)
      (response.getItems, response.getLastEvaluatedKey)
    }

    new Iterator[V] {
      private val iter = iterator("query: " + hashKeyValue, nextPage)
      override def hasNext = iter.hasNext
      override def next: V = itemMapper.unapply(iter.next)
    }
  }
}
