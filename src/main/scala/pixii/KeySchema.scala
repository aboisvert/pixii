package pixii

import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, KeySchemaElement}

sealed trait KeySchema[K] {
  val keySchema: List[KeySchemaElement]
  val attributeDefinitions: List[AttributeDefinition]
}

/** Enumeration of DynamoDB key schema types */
object KeySchema {
  case class HashKeySchema[H](hashKeyAttribute: NamedAttribute[H]) extends KeySchema[H] {
    override val keySchema =
      List[KeySchemaElement](
        new KeySchemaElement()
          .withAttributeName(hashKeyAttribute.name)
          .withKeyType(KeyTypes.Hash.code)
      )

    override val attributeDefinitions = List(hashKeyAttribute.definition)
  }

  case class HashAndRangeKeySchema[H, R](
    hashKeyAttribute: NamedAttribute[H],
    rangeKeyAttribute: NamedAttribute[R]
  ) extends KeySchema[(H, R)] {
    override val keySchema =
      List[KeySchemaElement](
          new KeySchemaElement()
            .withAttributeName(hashKeyAttribute.name)
            .withKeyType(KeyTypes.Hash.code),
          new KeySchemaElement()
            .withAttributeName(rangeKeyAttribute.name)
            .withKeyType(KeyTypes.Range.code)
      )

    override val attributeDefinitions = List(
      hashKeyAttribute.definition,
      rangeKeyAttribute.definition
    )
  }
}
