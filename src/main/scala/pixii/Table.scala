package pixii

import pixii.KeySchema._

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.{ TableStatus => AWSTableStatus }

import scala.collection._
import scala.collection.mutable.SynchronizedBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

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
  def toKey(k: K): Map[String, AttributeValue]

  /** Retrieve value associated with given key */
  def apply(key: K)(implicit consistency: Consistency): Option[V] = {
    retryPolicy.retry("Table(%s).apply(%s)" format (tableName, key)) {
      try {
        val request = (new GetItemRequest()
          .withTableName(tableName)
          .withKey(toKey(key).asJava)
          .withConsistentRead(consistency.isConsistentRead)
        )
        val response = dynamoDB.getItem(request)
        Option(response.getItem) map { item => itemMapper.unapply(item.asScala) }
      } catch { case e: ResourceNotFoundException =>
        return None
      }
    }
  }

  /** Delete value from table */
  def delete(key: K): Unit = {
    retryPolicy.retry("Table(%s).delete(%s)" format (tableName, key)) {
      val request = (new DeleteItemRequest()
        .withTableName(tableName)
          .withKey(toKey(key).asJava)
      )
      val response = dynamoDB.deleteItem(request)
    }
  }

  /** Put value into table */
  def put(value: V): Unit = {
    retryPolicy.retry("Table(%s).put(%s)" format (tableName, value)) {
      val request = (new PutItemRequest()
        .withTableName(tableName)
        .withItem(itemMapper.apply(value).asJava)
      )
      val response = dynamoDB.putItem(request)
    }
  }

  /** Update an item */
  def update(key: K, attributeUpdates: Map[String, AttributeValueUpdate]): Unit = {
    retryPolicy.retry("Table(%s).update(%s)" format (tableName, key)) {
      val request = (new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(toKey(key).asJava)
        .withAttributeUpdates(attributeUpdates.asJava))
      dynamoDB.updateItem(request)
    }
  }

  def updateAndReturnValues(key: K, attributeUpdates: Map[String, AttributeValueUpdate], returnValue: ReturnValue): Map[String, AttributeValue] = {
    retryPolicy.retry("Table(%s).update(%s)" format (tableName, key)) {
      val request = (new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(toKey(key).asJava)
        .withAttributeUpdates(attributeUpdates.asJava))
        .withReturnValues(returnValue)
      val response = dynamoDB.updateItem(request)
      response.getAttributes.asScala
    }
  }

  // TODO:  queryCount()
  // TODO:  scanCount()

  def parallelScan(
      segment: Int,
      totalSegments: Int,
      filter: Map[String, Condition],
      evaluateItemPageLimit: Int
  ): Iterator[V] = {
    val request = new ScanRequest().withTableName(tableName).withTotalSegments(totalSegments).withSegment(segment)

    if (filter.nonEmpty) request.setScanFilter(filter.asJava)

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
      if (exclusiveStartKey.isDefined) request.setExclusiveStartKey(exclusiveStartKey.get)
      val response = dynamoDB.scan(request)
      (response.getItems, response.getLastEvaluatedKey)
    }

    new Iterator[V] {
      private val iter = iterator("scan", nextPage)
      override def hasNext = iter.hasNext
      override def next: V = itemMapper.unapply(iter.next)
    }
  }

  def scan(
    filter: Map[String, Condition] = Map.empty,
    evaluateItemPageLimit: Int = -1
  ): Iterator[V] = {
    val request = new ScanRequest().withTableName(tableName)

    if (filter.nonEmpty) request.setScanFilter(filter.asJava)

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
      if (exclusiveStartKey.isDefined) request.setExclusiveStartKey(exclusiveStartKey.get)
      val response = dynamoDB.scan(request)
      (response.getItems, response.getLastEvaluatedKey)
    }

    new Iterator[V] {
      private val iter = iterator("scan", nextPage)
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
      .withAttributesToGet(attributesToGet.asJava))

    if (filter.nonEmpty) request.setScanFilter(filter.asJava)

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
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

  def getAll(keys: K*)(implicit consistency: Consistency): Iterator[V] = {
    if (keys.isEmpty)
      return Iterator.empty
    val keysAndAttributes = (new KeysAndAttributes()
      .withKeys(List(keys:_*).map { key => toKey(key).asJava }.asJava)
      .withConsistentRead(consistency.isConsistentRead)
    )
    val request = (new BatchGetItemRequest()
      .withRequestItems(mutable.Map(tableName -> keysAndAttributes).asJava)
    )

    def nextPage(remainingKeys: Option[java.util.List[java.util.Map[String, AttributeValue]]]) = {
      if (remainingKeys.isDefined) keysAndAttributes.setKeys(remainingKeys.get)
      val response = dynamoDB.batchGetItem(request)
      val unprocessedKeys = Option(response.getUnprocessedKeys) flatMap (m => Option(m.get(tableName)))
      // Work around an AWS bug that does not return the correct attribute names in the list of unprocessed keys.
      val recoveredKeys = unprocessedKeys map { unprocessedKeys =>
        val (hashKeyName, rangeKeyName) = schema match {
          case KeySchema.HashKeySchema(hash) => hash.name -> None
          case s: KeySchema.HashAndRangeKeySchema[_, _] => s.hashKeyAttribute.name -> Some(s.rangeKeyAttribute.name)
        }
        unprocessedKeys.getKeys.asScala map { key =>
          key.asScala collect {
            case (name, value) if name == "HashKeyElement" =>
              println(s"""AWS returned "HashKeyElement" as a key attribute name instead of "$hashKeyName"""")
              hashKeyName -> value
            case (name, value) if rangeKeyName.nonEmpty && name == "RangeKeyElement" =>
              rangeKeyName.get -> value
            case entry =>
              entry
          } asJava
        }
      }
      // End AWS workaround.
      (response.getResponses.get(tableName), recoveredKeys map { _ asJava } orNull)
    }

    new Iterator[V] {
      private val iter = iterator("getAll", nextPage)
      override def hasNext = iter.hasNext
      override def next: V = itemMapper.unapply(iter.next)
    }
  }

  def putAll(values: V*): WriteSequence = {
    putAll(values.iterator)
  }

  def putAll(values: Iterator[V]): WriteSequence = {
    writeAll(values map (WriteOperation.Put[K, V](_)))
  }

  def deleteAll(keys: K*): WriteSequence = {
    deleteAll(keys.iterator)
  }

  def deleteAll(keys: Iterator[K]): WriteSequence = {
    writeAll(keys map (WriteOperation.Delete[K, V](_)))
  }

  def writeAll(operations: WriteOperation[K, V]*): WriteSequence = {
    writeAll(operations.iterator)
  }

  def writeAll(operations: Iterator[WriteOperation[K, V]]): WriteSequence = {
    if (operations.isEmpty)
      return WriteSequence.empty
    val requests: java.util.List[WriteRequest] = new java.util.ArrayList[WriteRequest]()
    val request = (new BatchWriteItemRequest()
      .withRequestItems(mutable.Map(tableName -> requests).asJava)
    )

    def nextSubmission(unprocessed: java.util.List[WriteRequest], remaining: IndexedSeq[WriteOperation[K, V]]) = {
      val toAppend = remaining.take(25 - unprocessed.size).collect {
        case WriteOperation.Put(value) =>
          new WriteRequest().withPutRequest(new PutRequest().withItem(itemMapper(value).asJava))
        case WriteOperation.Delete(key) =>
          new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(toKey(key).asJava))
      }.toSeq
      requests.clear()
      requests.addAll(unprocessed)
      requests.addAll(toAppend.asJava)
      val response = retryPolicy.retry("Table(%s).writeAll" format tableName) {
        dynamoDB.batchWriteItem(request)
      }
      val unprocessedItems = Option(response.getUnprocessedItems()) flatMap (m => Option(m.get(tableName)))
      if (unprocessedItems.isEmpty) {
        (requests.size, java.util.Collections.emptyList[WriteRequest](), remaining drop toAppend.size)
      } else
        (requests.size - unprocessedItems.get.size, unprocessedItems.get, remaining drop toAppend.size)
    }

    new WriteSequence {

      private var pending: IndexedSeq[WriteOperation[K,V]] = operations.toIndexedSeq
      private var unprocessed = java.util.Collections.emptyList[WriteRequest]()
      var completedOperations = 0
      override def hasRemainingOperations = pending.nonEmpty || !unprocessed.isEmpty
      override def submitMoreOperations() = {
        if (!hasRemainingOperations) throw new NoSuchElementException
        val (written, stillUnprocessed, stillPending) = nextSubmission(unprocessed, pending)
        completedOperations += written
        unprocessed = stillUnprocessed
        pending = stillPending
        written
      }

    }
  }

  protected def iterator[T](
    operationName: String,
    nextPage: Option[T] => (java.util.List[java.util.Map[String, AttributeValue]], T)
  ): Iterator[Map[String, AttributeValue]] = new Iterator[Map[String, AttributeValue]] {
    private var index = 0
    private var items: java.util.List[java.util.Map[String, AttributeValue]] = null
    private var morePages = true
    private var exclusiveStartKey: Option[T] = None

    override def hasNext = synchronized {
      if ((items == null || index >= items.size) && morePages) loadNextPage()
      (items != null && index < items.size)
    }

    override def next(): Map[String, AttributeValue] = synchronized {
      if (!hasNext) throw new IllegalStateException("hasNext is false")
      val item = items.get(index)
      index += 1
      item.asScala
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

  def describeTable(): Option[TableDescription[K]] = {
    try {
      val request = new DescribeTableRequest().withTableName(tableName)
      val response = dynamoDB.describeTable(request)
      val table = response.getTable
      val provisionedThroughput = table.getProvisionedThroughput
      Some(TableDescription(
        tableName,
        schema,
        AWSTableStatus.valueOf(table.getTableStatus) match {
          case AWSTableStatus.CREATING => TableStatus.Creating
          case AWSTableStatus.UPDATING => TableStatus.Updating
          case AWSTableStatus.DELETING => TableStatus.Deleting
          case AWSTableStatus.ACTIVE => TableStatus.Active
        },
        table.getCreationDateTime,
        ProvisionedThroughputDescription(
          provisionedThroughput.getLastIncreaseDateTime,
          provisionedThroughput.getLastDecreaseDateTime,
          provisionedThroughput.getNumberOfDecreasesToday,
          provisionedThroughput.getReadCapacityUnits,
          provisionedThroughput.getWriteCapacityUnits),
        table.getTableSizeBytes,
        table.getItemCount))
    } catch {
      case e: ResourceNotFoundException => None
    }
  }

  def createTable(readCapacity: Long, writeCapacity: Long): Unit = {
    val provisionedThroughput = (new ProvisionedThroughput()
      .withReadCapacityUnits(readCapacity)
      .withWriteCapacityUnits(writeCapacity)
    )

    val request = (new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(schema.keySchema.asJava)
      .withAttributeDefinitions(schema.attributeDefinitions.asJava)
      .withProvisionedThroughput(provisionedThroughput)
    )
    dynamoDB.createTable(request)
  }

  def updateTable(readCapacity: Long, writeCapacity: Long): Unit = {
    val provisionedThroughput = (new ProvisionedThroughput()
      .withReadCapacityUnits(readCapacity)
      .withWriteCapacityUnits(writeCapacity)
    )
    val request = (new UpdateTableRequest()
      .withTableName(tableName)
      .withProvisionedThroughput(provisionedThroughput)
    )
    dynamoDB.updateTable(request)
  }

  def createOrUpdateTable(readCapacity: Long, writeCapacity: Long) = {
    def isDifferentThroughput(description: TableDescription[_]): Boolean = {
      description.provisionedThroughput.readCapacityUnits != readCapacity ||
        description.provisionedThroughput.writeCapacityUnits != writeCapacity
    }
    describeTable() match {
      case Some(description) => if (isDifferentThroughput(description)) updateTable(readCapacity, writeCapacity)
      case None => createTable(readCapacity, writeCapacity)
    }
  }

  def deleteTable_!(): Unit = {
    val deleteTableRequest = new DeleteTableRequest().withTableName(tableName)
    dynamoDB.deleteTable(deleteTableRequest)
  }

  def isTableActive: Boolean = {
    describeTable().map(d => d.tableStatus == TableStatus.Active).getOrElse(false)
  }
}

trait HashKeyTable[H, V] extends TableOperations[H, V] { self: Table[V] =>
  val hashKeyAttribute: NamedAttribute[H]

  override lazy val schema = HashKeySchema[H](hashKeyAttribute)
  override def toKey(key: H): Map[String, AttributeValue] = hashKeyAttribute(key)
}

trait HashAndRangeKeyTable[H, R, V] extends TableOperations[(H, R), V] { self: Table[V] =>
  val hashKeyAttribute: NamedAttribute[H]
  val rangeKeyAttribute: NamedAttribute[R]

  override lazy val schema = HashAndRangeKeySchema[H, R](hashKeyAttribute, rangeKeyAttribute)
  override def toKey(key: (H, R)): Map[String, AttributeValue] = hashKeyAttribute(key._1) ++ rangeKeyAttribute(key._2)

  def query(
    hashKeyValue: H,
    rangeKeyCondition: Condition = null,
    evaluateItemPageLimit: Int = -1
  )(implicit consistency: Consistency): Iterator[V] = {
    val keyConditions = mutable.Map[String, Condition]()
    keyConditions(hashKeyAttribute.name) = new Condition()
      .withAttributeValueList(hashKeyAttribute(hashKeyValue).valuesIterator.next())
      .withComparisonOperator(ComparisonOperator.EQ)
    if (rangeKeyCondition != null) {
      keyConditions(rangeKeyAttribute.name) = rangeKeyCondition
    }
    val request = (new QueryRequest()
      .withTableName(tableName)
      .withKeyConditions(keyConditions.asJava)
      .withConsistentRead(consistency.isConsistentRead))

    if (evaluateItemPageLimit != -1) request.setLimit(evaluateItemPageLimit)
    else request.setLimit(1000)

    def nextPage(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
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

case class TableDescription[K](
  tableName: String,
  keySchema: KeySchema[K],
  tableStatus: TableStatus,
  creationDateTime: java.util.Date,
  provisionedThroughput: ProvisionedThroughputDescription,
  tableSizeBytes: Long,
  itemCount: Long)

case class ProvisionedThroughputDescription(
  lastIncreaseDateTime: java.util.Date,
  lastDecreaseDateTime: java.util.Date,
  numberOfDecreasesToday: Long,
  readCapacityUnits: Long,
  writeCapacityUnits: Long)

sealed abstract class TableStatus

object TableStatus {
  case object Creating extends TableStatus
  case object Updating extends TableStatus
  case object Deleting extends TableStatus
  case object Active extends TableStatus
}

sealed abstract class WriteOperation[+K, +V]

object WriteOperation {
  case class Put[K, V](value: V) extends WriteOperation[K, V]
  case class Delete[K, V](key: K) extends WriteOperation[K, V]
}

trait WriteSequence {
  def completedOperations: Int
  def hasRemainingOperations: Boolean
  def submitMoreOperations(): Int
}

object WriteSequence {
  val empty = new WriteSequence {
    override def completedOperations = 0
    override def hasRemainingOperations = false
    override def submitMoreOperations(): Int =
      throw new NoSuchElementException
  }
}
