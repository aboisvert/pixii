package pixii.fake

import com.amazonaws._
import com.amazonaws.regions.Region
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

import scala.collection._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import pixii.KeyTypes

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
    new ListTablesResult().withTableNames(_tables.keySet)
  }

  override def query(queryRequest: QueryRequest): QueryResult = {
    val table = getTable(queryRequest.getTableName)
    table match {
      case table: FakeTableWithHashRangeKey => table.queryItem(queryRequest)
      case _ => throw new AmazonServiceException("Table %s does not support query method" format queryRequest.getTableName)
    }
  }

  override def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): BatchWriteItemResult = synchronized {
    val requests: Map[String, java.util.List[WriteRequest]] = batchWriteItemRequest.getRequestItems
    val responses = mutable.ArrayBuffer[ConsumedCapacity]()
    for ((tableName, writeRequests) <- requests) {
      val table = getTable(tableName)
      for (request <- writeRequests) {
        if (request.getPutRequest != null) {
          table.putItem(new PutItemRequest().withItem(request.getPutRequest.getItem))
        } else if (request.getDeleteRequest != null) {
          table.deleteItem(new DeleteItemRequest().withKey(request.getDeleteRequest.getKey))
        }
      }
      responses += new ConsumedCapacity().withTableName(tableName)
    }
    new BatchWriteItemResult().withConsumedCapacity(responses)
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
      if (createTableRequest.getKeySchema().filter(_.getKeyType == KeyTypes.Range.code).size == 0) {
        val table = new FakeTableWithHashKey(
          name, createTableRequest.getKeySchema, createTableRequest.getProvisionedThroughput)
        _tables.getOrElseUpdate(name, table)
        table
      } else {
        val table = new FakeTableWithHashRangeKey(
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
    val requests: Map[String, KeysAndAttributes] = batchGetItemRequest.getRequestItems
    val responses = mutable.Map[String, java.util.List[java.util.Map[String, AttributeValue]]]()
    for ((tableName, keysAndAttributes) <- requests) {
      val table = getTable(tableName)
      val items = for (key <- keysAndAttributes.getKeys) yield {
        table.getItem(new GetItemRequest().withKey(key)).getItem()
      }
      responses += tableName -> items
    }
    new BatchGetItemResult().withResponses(responses)
  }

  override def listTables(): ListTablesResult = synchronized {
    new ListTablesResult().withTableNames(_tables.keySet)
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
  def putItem(putItemRequest: PutItemRequest): PutItemResult
  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult
  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult
  def scan(scanRequest: ScanRequest): ScanResult
  def dump(buf: StringBuilder): Unit

  def updateItem(updateItemRequest: UpdateItemRequest, item: mutable.Map[String, AttributeValue]): UpdateItemResult = {
    for ((attr, update) <- updateItemRequest.getAttributeUpdates) {
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
                val sum = (value.getSS ++ update.getValue.getSS).distinct
                new AttributeValue().withSS(sum)
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
                val sum = (value.getNS ++ update.getValue.getNS).distinct
                new AttributeValue().withNS(sum)
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
                val leftover = (value.getSS -- update.getValue.getSS)
                new AttributeValue().withSS(leftover)
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
                val leftover = (value.getNS -- update.getValue.getNS)
                new AttributeValue().withNS(leftover)
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
          .withWriteCapacityUnits(provisionedThroughput.getWriteCapacityUnits))

}

class FakeTableWithHashKey(
  name: String,
  keySchema: java.util.List[KeySchemaElement],
  provisionedThroughput: ProvisionedThroughput)
  extends FakeTable(name, keySchema, provisionedThroughput) {
  val items = mutable.Map[Map[String, AttributeValue], mutable.Map[String, AttributeValue]]()

  def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val key = getItemRequest.getKey
    val item = items.get(key) getOrElse { throw new ResourceNotFoundException("Item not found: " + key) }
    new GetItemResult().withItem(item)
  }

  def putItem(putItemRequest: PutItemRequest): PutItemResult = {
    val key = putItemRequest.getItem
    val item = items.getOrElseUpdate(key, mutable.Map())
    item.clear()
    item ++= putItemRequest.getItem
    new PutItemResult()
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val key = deleteItemRequest.getKey
    val item = items.remove(key) getOrElse { return new DeleteItemResult() }
    new DeleteItemResult().withAttributes(item)
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = {
    val key = updateItemRequest.getKey
    val hashKey = keySchema.filter(_.getKeyType == KeyTypes.Hash.code).get(0).getAttributeName()
    val item: mutable.Map[String, AttributeValue] = items.getOrElseUpdate(key,
      mutable.Map(hashKey -> key(hashKey))
    )
    updateItem(updateItemRequest, item)
  }

  def scan(scanRequest: ScanRequest): ScanResult = {
    if (scanRequest.getScanFilter == null || scanRequest.getScanFilter.isEmpty()) {
      (new ScanResult).withItems(asJavaCollection(items.values map mutableMapAsJavaMap))
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

  override def describe(status: TableStatus): TableDescription =
    super.describe(status).withItemCount(items.size)

}

class FakeTableWithHashRangeKey(
  name: String,
  keySchema: java.util.List[KeySchemaElement],
  provisionedThroughput: ProvisionedThroughput)
  extends FakeTable(name, keySchema, provisionedThroughput) {
  val items = mutable.Map[AttributeValue, mutable.Map[AttributeValue, mutable.Map[String, AttributeValue]]]()

  def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val key = getItemRequest.getKey
    val hashKey = keySchema.filter(_.getKeyType == KeyTypes.Hash.code).get(0).getAttributeName()
    val rangeKey = keySchema.filter(_.getKeyType == KeyTypes.Range.code).get(0).getAttributeName()
    val range = items.get(key(hashKey)) getOrElse { throw new ResourceNotFoundException("Item not found: " + key) }
    val item = range.get(key(rangeKey)) getOrElse { throw new ResourceNotFoundException("Item not found: " + key) }
    new GetItemResult().withItem(item)
  }

  def putItem(putItemRequest: PutItemRequest): PutItemResult = {
    val range = items.getOrElseUpdate(putItemRequest.getItem.get(keySchema.filter(_.getKeyType() == KeyTypes.Hash.code).get(0).getAttributeName), mutable.Map())
    val item = range.getOrElseUpdate(putItemRequest.getItem.get(keySchema.filter(_.getKeyType() == KeyTypes.Range.code).get(0).getAttributeName), mutable.Map())
    item.clear()
    item ++= putItemRequest.getItem
    new PutItemResult()
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val key = deleteItemRequest.getKey
    val hashKey = keySchema.filter(_.getKeyType == KeyTypes.Hash.code).get(0).getAttributeName()
    val rangeKey = keySchema.filter(_.getKeyType == KeyTypes.Range.code).get(0).getAttributeName()
    val range = items.get(key(hashKey)) getOrElse { return new DeleteItemResult() }
    val item = range.remove(key(rangeKey)) getOrElse { return new DeleteItemResult() }
    new DeleteItemResult().withAttributes(item)
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = {
    val key = updateItemRequest.getKey
    val hashKey = keySchema.filter(_.getKeyType == KeyTypes.Hash.code).get(0).getAttributeName()
    val rangeKey = keySchema.filter(_.getKeyType == KeyTypes.Range.code).get(0).getAttributeName()
    val range = items.getOrElseUpdate(key(hashKey), mutable.Map())
    val item: mutable.Map[String, AttributeValue] = range.getOrElseUpdate(key(rangeKey),
      mutable.Map(
        hashKey -> key(hashKey),
        rangeKey -> key(rangeKey)
      )
    )
    updateItem(updateItemRequest, item)
  }

  def queryItem(queryRequest: QueryRequest): QueryResult = {
    val result = new QueryResult()
    val hashKeyConditions = queryRequest.getKeyConditions().getOrElse(KeyTypes.Hash.code, new Condition())
    val _items = items.getOrElse(hashKeyConditions.getAttributeValueList().iterator().next(), mutable.Map()).values.toList
    result.withItems(_items.asJava map { _.asJava })
  }

  def scan(scanRequest: ScanRequest): ScanResult = {
    if (scanRequest.getScanFilter.isEmpty()) {
      new ScanResult().withItems(items.values.toList.asJava flatMap { _.values.toList } map mutableMapAsJavaMap)
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
