package pixii

import com.amazonaws.services.dynamodb.model.AttributeValue
import pixii.AttributeValueConversions._
import scala.collection._
import scala.collection.JavaConversions._

abstract class MapAttributes[K, V](
  val prefix: String,
  val separator: Char = ':'
)(implicit 
  val keyConversion: AttributeNameConversion[K], 
  val valueConversion: AttributeValueConversion[V]
) extends Attribute {
  
  type ATTR = Map[K, V]

  private val fullPrefix = prefix + separator
  override def apply(value: Map[K, V]): Map[String, AttributeValue] = {
    for ((k, v) <- value; attributeValue <- valueConversion(v)) yield {
      val attributeName = fullPrefix + keyConversion(k) 
      attributeName -> attributeValue
    }
  }
  override def unapply(item: Map[String, AttributeValue]): Option[Map[K, V]] = {
    val result = mutable.Map[K, V]()
    for ((attrName, attrValue) <- item) {
      if (attrName startsWith fullPrefix) {
        val suffix = attrName.substring(fullPrefix.length)
        val key = keyConversion.unapply(suffix)
        val value = valueConversion.unapply(attrValue)
        result(key) = value
      }
    }
    Some(result)
  }
  override def toString = "MapAttributes(prefix = '%s', separator = '%s')" format (prefix, separator)
}

trait AttributeNameConversion[T] {
  def apply(value: T): String
  def unapply(s: String): T 
}

object AttributeNameConversions {
  private def wrapException[T](f: => T) = {
    try { f } catch { case e: Exception => throw new ValidationException(e) }
  }
  
  implicit object StringMapAttribute extends AttributeNameConversion[String] {
    override def apply(value: String) = value
    override def unapply(s: String) = s
  }

  implicit object IntMapAttribute extends AttributeNameConversion[Int] {
    override def apply(value: Int) = wrapException(value.toString)
    override def unapply(s: String) = wrapException(s.toInt)
  }

  implicit object LongMapAttribute extends AttributeNameConversion[Long] {
    override def apply(value: Long) = wrapException(value.toString)
    override def unapply(s: String) = wrapException(s.toLong)
  }
}