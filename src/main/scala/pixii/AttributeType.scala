package pixii

sealed class AttributeType(val code: String)

/** Enumeration of all DynamoDB attribute types */
object AttributeTypes {
  object Binary extends AttributeType("B")
  object Number extends AttributeType("N")
  object String extends AttributeType("S")

  object StringSet extends AttributeType("SS")
  object NumberSet extends AttributeType("NS")

  val enumeration = Set(Binary, Number, String, StringSet, NumberSet)
}

