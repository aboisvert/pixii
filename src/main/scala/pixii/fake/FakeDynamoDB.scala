package pixii.fake

import com.amazonaws._
import com.amazonaws.auth._
import com.amazonaws.services.dynamodb._
import com.amazonaws.services.dynamodb.model._

import scala.collection._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class FakeDynamo extends AmazonDynamoDB {

  var _endpoint: String = _

  private val _tables = mutable.Map[String, FakeTable]()

  // PUBLIC API

  def setEndpoint(endpoint: String) {
    this._endpoint = endpoint
  }

  def listTables(listTablesRequest: ListTablesRequest): ListTablesResult = synchronized {
    new ListTablesResult().withTableNames(_tables.keySet)
  }

  def query(queryRequest: QueryRequest): QueryResult = {
    val table = getTable(queryRequest.getTableName)
    table match { 
      case table: FakeTableWithHashRangeKey => table.queryItem(queryRequest)
      case _ => throw new AmazonServiceException("Table %s does not support query method" format queryRequest.getTableName)
    }
  }

  def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): BatchWriteItemResult = {
    throw new UnsupportedOperationException
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = synchronized {
    val table = getTable(updateItemRequest.getTableName)
    table.updateItem(updateItemRequest)
  }

  def putItem(putItemRequest: PutItemRequest): PutItemResult = {
    throw new UnsupportedOperationException
  }

  def describeTable(describeTableRequest: DescribeTableRequest): DescribeTableResult = {
    throw new UnsupportedOperationException
  }

  def scan(scanRequest: ScanRequest): ScanResult = {
    throw new UnsupportedOperationException
  }

  def createTable(createTableRequest: CreateTableRequest): CreateTableResult = synchronized {
    val name = createTableRequest.getTableName
    if (_tables.isDefinedAt(name)) {
      throw new AmazonServiceException("Table already exists: " + name)
    }
    if (createTableRequest.getKeySchema.getRangeKeyElement == null) { 
      _tables.getOrElseUpdate(name, new FakeTableWithHashKey(name, createTableRequest.getKeySchema))
    } else {
      _tables.getOrElseUpdate(name, new FakeTableWithHashRangeKey(name, createTableRequest.getKeySchema))
    }    
    new CreateTableResult()
  }

  def updateTable(updateTableRequest: UpdateTableRequest): UpdateTableResult = {
    throw new UnsupportedOperationException
  }

  def deleteTable(deleteTableRequest: DeleteTableRequest): DeleteTableResult = {
    throw new UnsupportedOperationException
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val table = getTable(deleteItemRequest.getTableName)
    table.deleteItem(deleteItemRequest)
  }

  def getItem(getItemRequest: GetItemRequest): GetItemResult = synchronized {
    val table = getTable(getItemRequest.getTableName)
    table.getItem(getItemRequest)
  }

  def batchGetItem(batchGetItemRequest: BatchGetItemRequest): BatchGetItemResult = {
    throw new UnsupportedOperationException
  }

  def listTables(): ListTablesResult = {
    throw new UnsupportedOperationException
  }

  def shutdown(): Unit = ()

  def getCachedResponseMetadata(request: AmazonWebServiceRequest): ResponseMetadata = {
    throw new UnsupportedOperationException
  }

  private def getTable(name: String): FakeTable = synchronized {
    _tables.get(name) getOrElse { throw new ResourceNotFoundException("Table not found: " + name) }
  }

}



abstract class FakeTable(val name: String, val keySchema: KeySchema) {
  def getItem(getItemRequest: GetItemRequest): GetItemResult
  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult
  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult
  
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
  
}


class FakeTableWithHashKey(name: String, keySchema: KeySchema) extends FakeTable(name, keySchema) {
  val items = mutable.Map[Key, mutable.Map[String, AttributeValue]]()

  def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val key = getItemRequest.getKey
    val item = items.get(key) getOrElse { throw new ResourceNotFoundException("Item not found: " + key) }
    new GetItemResult().withItem(item)
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val key = deleteItemRequest.getKey
    val item = items.remove(key) getOrElse { return new DeleteItemResult() }
    new DeleteItemResult().withAttributes(item)
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = {
    val key = updateItemRequest.getKey
    val item: mutable.Map[String, AttributeValue] = items.getOrElseUpdate(key,
      mutable.Map(keySchema.getHashKeyElement.getAttributeName -> key.getHashKeyElement)
    )
    updateItem(updateItemRequest, item)
  }
}

class FakeTableWithHashRangeKey(name: String, keySchema: KeySchema) extends FakeTable(name, keySchema) {
  val items = mutable.Map[AttributeValue, mutable.Map[AttributeValue, mutable.Map[String, AttributeValue]]]()

  def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val key = getItemRequest.getKey
    val range = items.get(key.getHashKeyElement) getOrElse { throw new ResourceNotFoundException("Item not found: " + key) }
    val item = range.get(key.getRangeKeyElement) getOrElse { throw new ResourceNotFoundException("Item not found: " + key) }
    new GetItemResult().withItem(item)
  }

  def deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResult = {
    val key = deleteItemRequest.getKey
    val range = items.get(key.getHashKeyElement) getOrElse { return new DeleteItemResult() }
    val item = range.remove(key.getRangeKeyElement) getOrElse { return new DeleteItemResult() }
    new DeleteItemResult().withAttributes(item)
  }

  def updateItem(updateItemRequest: UpdateItemRequest): UpdateItemResult = {
    val key = updateItemRequest.getKey
    val range = items.getOrElseUpdate(key.getHashKeyElement, mutable.Map())
    val item: mutable.Map[String, AttributeValue] = range.getOrElseUpdate(key.getRangeKeyElement,
      mutable.Map(
        keySchema.getHashKeyElement.getAttributeName -> key.getHashKeyElement,
        keySchema.getRangeKeyElement.getAttributeName -> key.getRangeKeyElement
      )
    )
    updateItem(updateItemRequest, item)
  }
  
  def queryItem(queryRequest: QueryRequest): QueryResult = {
    val result = new QueryResult()
    val _items = items.getOrElse(queryRequest.getHashKeyValue, mutable.Map()).values
    result.withItems(_items map { m => m: java.util.Map[String, AttributeValue] })
  }  
    
}
