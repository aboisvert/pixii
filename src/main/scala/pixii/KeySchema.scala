package pixii

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement

sealed trait KeySchema[K] {
  val keySchema: List[KeySchemaElement]
}

/** Enumeration of DynamoDB key schema types */
object KeySchema {
  case class HashKeySchema[H](hashKeyAttribute: NamedAttribute[H]) extends KeySchema[H] {
    val keySchema = 
      List[KeySchemaElement](
        new KeySchemaElement()
          .withAttributeName(hashKeyAttribute.name)
          .withKeyType(KeyTypes.Hash.code)
      )
  }

  case class HashAndRangeKeySchema[H, R](
    hashKeyAttribute: NamedAttribute[H],
    rangeKeyAttribute: NamedAttribute[R]
  ) extends KeySchema[(H, R)] {
    val keySchema = 
      List[KeySchemaElement](
          new KeySchemaElement()
            .withAttributeName(hashKeyAttribute.name)
            .withKeyType(KeyTypes.Hash.code),
          new KeySchemaElement()
            .withAttributeName(rangeKeyAttribute.name)
            .withKeyType(KeyTypes.Range.code)
      )
  }
}
