package pixii

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import scala.collection._
import scala.collection.JavaConversions._

trait Attribute {
  type ATTR
  def apply(value: ATTR): Map[String, AttributeValue] = sys.error("must be overriden")
  def unapply(item: Map[String, AttributeValue]): Option[ATTR] = sys.error("must be overriden")

  def get(item: Map[String, AttributeValue]): ATTR
}

abstract class NamedAttribute[T](val name: String)(implicit val conversion: AttributeValueConversion[T]) extends Attribute {
  type ATTR = T
  override def apply(value: T): Map[String, AttributeValue] = {
    val convertedValue = conversion(value)
    if (convertedValue.isDefined) Map(name -> convertedValue.get) else Map.empty
  }
  override def unapply(item: Map[String, AttributeValue]): Option[T] = item.get(name) map conversion.unapply
  override def toString = "NamedAttribute(%s)" format name
}


object AttributeModifiers {

  trait Required extends Attribute {
    override def apply(value: ATTR): Map[String, AttributeValue] = {
      if (value == null) throw new RequiredAttributeCannotBeNullException(this.toString)
      super.apply(value)
    }

    override def get(item: Map[String, AttributeValue]) = {
      unapply(item) getOrElse { throw new MissingAttributeException(this.toString, item) }
    }
  }

  trait Optional extends Attribute {
    type ATTR <: Option[_]
    override def apply(value: ATTR): Map[String, AttributeValue] = {
      if (value == null) throw new RequiredAttributeCannotBeNullException(this.toString)
      super.apply(value)
    }

    override def get(item: Map[String, AttributeValue]) = {
      unapply(item) getOrElse { None.asInstanceOf[ATTR] }
    }
  }

  trait Nullable extends Attribute {
    override def get(item: Map[String, AttributeValue]) = unapply(item) getOrElse null.asInstanceOf[ATTR]
  }

  trait Default extends Attribute {
    val defaultValue: ATTR
    override def apply(value: ATTR): Map[String, AttributeValue] = {
      if (value != null) super.apply(value) else super.apply(defaultValue)
    }
    override def get(item: Map[String, AttributeValue]) = unapply(item) getOrElse defaultValue
  }

  trait SafeDefault extends Attribute {
    val safeValue: Option[ATTR]
    override def unapply(item: Map[String, AttributeValue]) = {
      try {
        super.unapply(item)
      } catch { case e: Exception =>
        safeValue
      }
    }
  }
}

class ValidationException(message: String, t: Throwable) extends RuntimeException(message, t) {
  def this(message: String) = this(message, null)
  def this(t: Throwable) = this(t.getMessage, t)
}

class MissingAttributeException(
  val attributeName: String,
  val item: Map[String, AttributeValue]
) extends ValidationException("Missing attribute '%s' in item %s" format (attributeName, item))

class RequiredAttributeCannotBeNullException(
  val attributeName: String
) extends ValidationException("Required attribute '%s' cannot be null." format attributeName)
