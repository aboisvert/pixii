package pixii

import com.amazonaws.services.dynamodb

sealed trait KeySchema[K] {
  val keySchema: dynamodb.model.KeySchema
}

/** Enumeration of DynamoDB key schema types */
object KeySchema {
  case class HashKeySchema[H](hashKeyAttribute: NamedAttribute[H]) extends KeySchema[H] {
    val keySchema = (new dynamodb.model.KeySchema()
      .withHashKeyElement(new dynamodb.model.KeySchemaElement()
        .withAttributeName(hashKeyAttribute.name)
        .withAttributeType(hashKeyAttribute.conversion.attributeType.code)))
  }
  
  case class HashAndRangeKeySchema[H, R](
    hashKeyAttribute: NamedAttribute[H], 
    rangeKeyAttribute: NamedAttribute[R]
  ) extends KeySchema[(H, R)] {
    val keySchema = (new dynamodb.model.KeySchema()
      .withHashKeyElement(new dynamodb.model.KeySchemaElement()
        .withAttributeName(hashKeyAttribute.name)
        .withAttributeType(hashKeyAttribute.conversion.attributeType.code))
      .withRangeKeyElement(new dynamodb.model.KeySchemaElement()
        .withAttributeName(rangeKeyAttribute.name)
        .withAttributeType(rangeKeyAttribute.conversion.attributeType.code)))
  }
}
