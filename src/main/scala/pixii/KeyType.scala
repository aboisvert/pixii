package pixii

sealed class KeyType(val code: String)

/** Enumeration of all DynamoDB key types */
object KeyTypes {
  object Hash extends KeyType("HASH")
  object Range extends KeyType("RANGE")

  val enumeration = Set(Hash, Range)
}

