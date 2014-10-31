package pixii.fake

import com.amazonaws._
import com.amazonaws.regions.Region
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import java.util.Date
import pixii.KeyTypes
import scala.collection._
import scala.collection.JavaConverters._


class FakeDynamo extends AmazonDynamoDB {

  var _endpoint: String = _
  var _region: Region = _

  private val _tables = mutable.Map[String, FakeTable]()

  // PUBLIC API

  override def setRegion(region: Region) {
    _region = region
  }

  override def setEndpoint(endpoint: String) {
    this._endpoint = endpoint
  }

  override def listTables(listTablesRequest: ListTablesRequest): ListTablesResult = synchronized {
    new ListTablesResult().withTableNames(_tables.keySet.asJava)
  }

  override def query(queryRequest: QueryRequest): QueryResult = {
    val table = getTable(queryRequest.getTableName)
    table match {
      case table: FakeTableWithHashRangeKey => table.queryItem(queryRequest)
      case _ => throw new AmazonServiceException("Table %s does not support query method" format queryRequest.getTableName)
    }
  }

  override def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): BatchWriteItemResult = synchronized {
    val requests: Map[String, java.util.List[WriteRequest]] = batchWriteItemRequest.getRequestItems.asScala
    val responses = mutable.ArrayBuffer[ConsumedCapacity]()
    for ((tableName, writeRequests) <- requests) {
      val table = getTable(tableName)
      for (request <- writeRequests.asScala) {
        if (request.getPutRequest != null) {
          table.putItem(new PutItemRequest().withItem(request.getPutRequest.getItem))
        } else if (request.getDeleteRequest != null) {
          table.deleteItem(new DeleteItemRequest().withKey(request.getDeleteRequest.getKey))
        }
      }
      responses += new ConsumedCapacity().withTableName(tableName)
    }
    new BatchWriteItemResult().withConsumedCapacity(responses.asJava)
  }

  override def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = synchronized {
    val table = getTable(updateItemRequest.getTableName)
    table.updateItem(updateItemRequest)
  }

  override def putItem(putItemRequest: PutItemRequest): PutItemResult = synchronized {
    val table = getTable(putItemRequest.getTableName)
    table.putItem(putItemRequest)
  }

  override def describeTable(describeTableRequest: DescribeTableRequest): DescribeTableResult = synchronized {
    val table = getTable(describeTableRequest.getTableName)
    new DescribeTableResult().withTable(table.describe(TableStatus.ACTIVE))
  }

  override def scan(scanRequest: ScanRequest): ScanResult = {
    val table = getTable(scanRequest.getTableName)
    table.scan(scanRequest)
  }

  override def createTable(createTableRequest: CreateTableRequest): CreateTableResult = synchronized {
    val name = createTableRequest.getTableName
    if (_tables.isDefinedAt(name)) {
      throw new AmazonServiceException("Table already exists: " + name)
    }
    val table =
      if (createTableRequest.getKeySchema.asScala exists (_.getKeyType == KeyTypes.Range.code)) {
        val table = new FakeTableWithHashRangeKey(
          name, createTableRequest.getKeySchema, createTableRequest.getProvisionedThroughput)
        _tables.getOrElseUpdate(name, table)
        table
      } else {
        val table = new FakeTableWithHashKey(
          name, createTableRequest.getKeySchema, createTableRequest.getProvisionedThroughput)
        _tables.getOrElseUpdate(name, table)
        table
      }
    new CreateTableResult().withTableDescription(table.describe(TableStatus.CREATING))
  }

  override def updateTable(updateTableRequest: UpdateTableRequest): UpdateTableResult = synchronized {
    val table = getTable(updateTableRequest.getTableName)
    table.provisionedThroughput = updateTableRequest.getProvisionedThroughput
    new UpdateTableResult().withTableDescription(table.describe(TableStatus.UPDATING))
  }

  override def deleteTable(deleteTableRequest: DeleteTableRequest): DeleteTableResult = synchronized {
    val table = getTable(deleteTableRequest.getTableName)
    new DeleteTableResult().withTableDescription(table.describe(TableStatus.DELETING))
  }

  override def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = synchronized {
    val table = getTable(deleteItemRequest.getTableName)
    table.deleteItem(deleteItemRequest)
  }

  override def getItem(getItemRequest: GetItemRequest): GetItemResult = synchronized {
    val table = getTable(getItemRequest.getTableName)
    table.getItem(getItemRequest)
  }

  override def batchGetItem(batchGetItemRequest: BatchGetItemRequest): BatchGetItemResult = synchronized {
    val requests: Map[String, KeysAndAttributes] = batchGetItemRequest.getRequestItems.asScala
    val responses = mutable.Map[String, java.util.List[java.util.Map[String, AttributeValue]]]()
    for ((tableName, keysAndAttributes) <- requests) {
      val table = getTable(tableName)
      val items = for {
        key <- keysAndAttributes.getKeys.asScala
        item <- table.getItemOpt(new GetItemRequest().withKey(key))
      } yield item.getItem()
      if (items.nonEmpty)
        responses += tableName -> items.asJava
    }
    new BatchGetItemResult().withResponses(responses.asJava)
  }

  override def listTables(): ListTablesResult = synchronized {
    new ListTablesResult().withTableNames(_tables.keySet.asJava)
  }

  override def shutdown(): Unit = ()

  override def getCachedResponseMetadata(request: AmazonWebServiceRequest): ResponseMetadata = {
    throw new UnsupportedOperationException
  }

  private def getTable(name: String): FakeTable = synchronized {
    _tables.get(name) getOrElse { throw new ResourceNotFoundException("Table not found: " + name) }
  }

  def dump(): String = {
    val buf = new StringBuilder
    for (t <- _tables.values) {
      t.dump(buf)
    }
    buf.toString
  }

  def dump(table: String): String = {
    val buf = new StringBuilder
    getTable(table).dump(buf)
    buf.toString
  }
}

abstract class FakeTable(
  val name: String,
  val keySchema: java.util.List[KeySchemaElement],
  var provisionedThroughput: ProvisionedThroughput) {
  val creationDateTime = new java.util.Date
  def getItem(getItemRequest: GetItemRequest): GetItemResult
  def getItemOpt(getItemRequest: GetItemRequest): Option[GetItemResult]
  def putItem(putItemRequest: PutItemRequest): PutItemResult
  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult
  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult
  def scan(scanRequest: ScanRequest): ScanResult
  def dump(buf: StringBuilder): Unit

  def updateItem(updateItemRequest: UpdateItemRequest, item: mutable.Map[String, AttributeValue]): UpdateItemResult = {
    for ((attr, update) <- updateItemRequest.getAttributeUpdates.asScala) {
      val value = item.get(attr) getOrElse null

      update.getAction match {
        case "ADD" =>
          if (update.getValue == null) {
            throw new AmazonServiceException("No value supplied for attribute '%s' with AttributeAction.ADD" format attr)
          } else if (update.getValue.getS != null) {
            throw new AmazonServiceException("Can't add strings")
          } else if (update.getValue.getN != null) {
            val newValue = {
              if (value == null) {
                update.getValue
              } else if (value.getN != null) {
                // todo: support adding decimals?
                val sum = value.getN.toLong + update.getValue.getN.toLong
                new AttributeValue().withN(sum.toString)
              } else {
                throw new AmazonServiceException("Existing value is not a number")
              }
            }
            item(attr) = newValue
          } else if (update.getValue.getSS != null) {
            val newValue = {
              if (value == null) {
                update.getValue
              } else if (value.getN != null || value.getS != null || value.getNS != null) {
                throw new AmazonServiceException("Existing value is not string set")
              } else if (value.getSS != null) {
                val sum = (value.getSS.asScala ++ update.getValue.getSS.asScala).distinct
                new AttributeValue().withSS(sum.asJava)
              } else {
                throw new AmazonServiceException("Unexpected")
              }
            }
            item(attr) = newValue
          } else if (update.getValue.getNS != null) {
            val newValue = {
              if (value == null) {
                update.getValue
              } else if (value.getN != null || value.getS != null || value.getSS != null) {
                throw new AmazonServiceException("Existing value is not string set")
              } else if (value.getNS != null) {
                val sum = (value.getNS.asScala ++ update.getValue.getNS.asScala).distinct
                new AttributeValue().withNS(sum.asJava)
              } else {
                throw new AmazonServiceException("Unexpected")
              }
            }
            item(attr) = newValue
          } else {
            sys.error("Unexpected value type: " + update.getValue)
          }

        case "PUT" =>
          item(attr) = update.getValue

        case "DELETE" =>
          if (update.getValue == null) {
            // no-value -> delete attribute entirely
            item.remove(attr)
          } else if (update.getValue.getS != null) {
            throw new AmazonServiceException("Can't delete strings")
          } else if (update.getValue.getN != null) {
            throw new AmazonServiceException("Can't delete number")
          } else if (update.getValue.getSS != null) {
            val newValue = {
              if (value == null) {
                update.getValue
              } else if (value.getN != null || value.getS != null || value.getNS != null) {
                throw new AmazonServiceException("Existing value is not string set")
              } else if (value.getSS != null) {
                val leftover = (value.getSS.asScala -- update.getValue.getSS.asScala)
                new AttributeValue().withSS(leftover.asJava)
              } else {
                throw new AmazonServiceException("Unexpected")
              }
            }
            item(attr) = newValue
          } else if (update.getValue.getNS != null) {
            val newValue = {
              if (value == null) {
                update.getValue
              } else if (value.getN != null || value.getS != null || value.getSS != null) {
                throw new AmazonServiceException("Existing value is not string set")
              } else if (value.getNS != null) {
                val leftover = (value.getNS.asScala -- update.getValue.getNS.asScala)
                new AttributeValue().withNS(leftover.asJava)
              } else {
                throw new AmazonServiceException("Unexpected")
              }
            }
            item(attr) = newValue
          } else {
            sys.error("Unexpected value type: " + update.getValue)
          }
      }
    }
    new UpdateItemResult()
  }

  def describe(status: TableStatus): TableDescription =
    new TableDescription()
      .withTableName(name)
      .withKeySchema(keySchema)
      .withTableStatus(status)
      .withCreationDateTime(creationDateTime)
      .withProvisionedThroughput(
        new ProvisionedThroughputDescription()
          .withReadCapacityUnits(provisionedThroughput.getReadCapacityUnits)
          .withWriteCapacityUnits(provisionedThroughput.getWriteCapacityUnits)
          .withLastDecreaseDateTime(new Date)
          .withLastIncreaseDateTime(new Date)
          .withNumberOfDecreasesToday(10L))


}

class FakeTableWithHashKey(
  name: String,
  keySchema: java.util.List[KeySchemaElement],
  provisionedThroughput: ProvisionedThroughput)
  extends FakeTable(name, keySchema, provisionedThroughput) {
  val items = mutable.Map[Map[String, AttributeValue], mutable.Map[String, AttributeValue]]()

  val hashKey = keySchema.asScala find (_.getKeyType == KeyTypes.Hash.code) get;

  def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val key = getItemRequest.getKey
    getItemOpt(getItemRequest) getOrElse { new GetItemResult() }
  }

  def getItemOpt(getItemRequest: GetItemRequest): Option[GetItemResult] = {
    val key = getItemRequest.getKey.asScala
    items.get(key) map { item => new GetItemResult().withItem(item.asJava) }
  }

  def putItem(putItemRequest: PutItemRequest): PutItemResult = {
    val key = putItemRequest.getItem.asScala filterKeys (_ == hashKey.getAttributeName)
    val item = items.getOrElseUpdate(key, mutable.Map())
    item.clear()
    item ++= putItemRequest.getItem.asScala
    new PutItemResult()
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val key = deleteItemRequest.getKey.asScala
    val item = items.remove(key) getOrElse { return new DeleteItemResult() }
    new DeleteItemResult().withAttributes(item.asJava)
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = {
    val key = updateItemRequest.getKey.asScala
    val item: mutable.Map[String, AttributeValue] = items.getOrElseUpdate(key,
      mutable.Map(hashKey.getAttributeName -> key(hashKey.getAttributeName))
    )
    updateItem(updateItemRequest, item)
  }

  def scan(scanRequest: ScanRequest): ScanResult = {
    if (scanRequest.getScanFilter == null || scanRequest.getScanFilter.isEmpty()) {
      (new ScanResult).withItems(items.values map (_.asJava) asJavaCollection)
    } else {
      sys.error("Scan with filter not yet supported")
    }
  }

  def dump(buf: StringBuilder) = {
    buf.append("FakeTableWithHashKey(%s, %s)\n" format (name, keySchema))
    for (i <- items) {
      buf.append("  " + i + "\n")
    }
  }

  override def describe(status: TableStatus): TableDescription = {
    val tableSize = items.map { case (keys: Map[String, AttributeValue], values: Map[String, AttributeValue]) =>
      val keysSize = keys.map(_._1.getBytes.size).sum
      val valuesSize = values.map(_._1.getBytes.size).sum
      keysSize + valuesSize
    }.sum
    super.describe(status).withItemCount(items.size).withTableSizeBytes(tableSize)
  }

}

class FakeTableWithHashRangeKey(
  name: String,
  keySchema: java.util.List[KeySchemaElement],
  provisionedThroughput: ProvisionedThroughput)
  extends FakeTable(name, keySchema, provisionedThroughput) {

  val items = mutable.Map[AttributeValue, mutable.Map[AttributeValue, mutable.Map[String, AttributeValue]]]()

  val hashKey = keySchema.asScala find (_.getKeyType == KeyTypes.Hash.code) get;
  val rangeKey = keySchema.asScala find (_.getKeyType == KeyTypes.Range.code) get;


  def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val key = getItemRequest.getKey
    getItemOpt(getItemRequest) getOrElse { new GetItemResult() }
  }

  def getItemOpt(getItemRequest: GetItemRequest): Option[GetItemResult] = {
    val key = getItemRequest.getKey.asScala
    items.get(key(hashKey.getAttributeName))
      .flatMap (_.get(key(rangeKey.getAttributeName)))
      .map { item => new GetItemResult().withItem(item.asJava) }
  }

  def putItem(putItemRequest: PutItemRequest): PutItemResult = {
    val range = items.getOrElseUpdate(putItemRequest.getItem.get(hashKey.getAttributeName), mutable.Map())
    val item = range.getOrElseUpdate(putItemRequest.getItem.get(rangeKey.getAttributeName), mutable.Map())
    item.clear()
    item ++= putItemRequest.getItem.asScala
    new PutItemResult()
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val key = deleteItemRequest.getKey.asScala
    val range = items.get(key(hashKey.getAttributeName)) getOrElse { return new DeleteItemResult() }
    val item = range.remove(key(rangeKey.getAttributeName)) getOrElse { return new DeleteItemResult() }
    new DeleteItemResult().withAttributes(item.asJava)
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = {
    val key = updateItemRequest.getKey.asScala
    val range = items.getOrElseUpdate(key(hashKey.getAttributeName), mutable.Map())
    val item: mutable.Map[String, AttributeValue] = range.getOrElseUpdate(key(rangeKey.getAttributeName),
      mutable.Map(
        hashKey.getAttributeName -> key(hashKey.getAttributeName),
        rangeKey.getAttributeName -> key(rangeKey.getAttributeName)
      )
    )
    updateItem(updateItemRequest, item)
  }

  def queryItem(queryRequest: QueryRequest): QueryResult = {
    val result = new QueryResult()
    val hashKeyConditions = queryRequest.getKeyConditions.asScala getOrElse (hashKey.getAttributeName, new Condition())
    val _items = items.getOrElse(hashKeyConditions.getAttributeValueList().iterator().next(), mutable.Map()).values.toList
    result.withItems(_items map (_.asJava) asJavaCollection)
  }

  def scan(scanRequest: ScanRequest): ScanResult = {
    if (scanRequest.getScanFilter.isEmpty()) {
      new ScanResult().withItems(items.values.toList flatMap (_.values.toList) map (_.asJava) asJavaCollection)
    } else {
      sys.error("Scan with filter not yet supported")
    }
  }

  def dump(buf: StringBuilder) = {
    buf.append("FakeTableWithHashAndRangeKey(%s, %s)\n" format (name, keySchema))
    for (i <- items) {
      buf.append("  " + i + "\n")
    }
  }

  override def describe(status: TableStatus): TableDescription =
    super.describe(status).withItemCount(items.values.map(_.size).sum)

}
