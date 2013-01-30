package pixii

import com.amazonaws.services.dynamodb.model.AttributeValue

import scala.collection._
import scala.collection.JavaConversions._

trait AttributeValueConversion[T] {
  val attributeType: AttributeType
  def apply(value: T): Option[AttributeValue]
  def unapply(value: AttributeValue): T
}

object AttributeValueConversions {
  implicit object PassThrough extends AttributeValueConversion[String] {
    override val attributeType = AttributeTypes.String
    override def apply(value: String) = Option(value) map (new AttributeValue().withS)
    override def unapply(value: AttributeValue) = value.getS
  }

  implicit object IntConversion extends AttributeValueConversion[Int] {
    override val attributeType = AttributeTypes.Number
    override def apply(value: Int) = Some(new AttributeValue().withN(value.toString))
    override def unapply(value: AttributeValue) = {
      if (value.getN == null) null.asInstanceOf[Int] else value.getN.toInt
    }
  }

  implicit object LongConversion extends AttributeValueConversion[Long] {
    override val attributeType = AttributeTypes.Number
    override def apply(value: Long) = Some(new AttributeValue().withN(value.toString))
    override def unapply(value: AttributeValue) = {
      if (value.getN == null) null.asInstanceOf[Long] else value.getN.toLong
    }
  }

  implicit def optionAttributeValueConversion[T](implicit conversion: AttributeValueConversion[T]) = new AttributeValueConversion[Option[T]] {
    override val attributeType = conversion.attributeType
    override def apply(value: Option[T]): Option[AttributeValue] = value flatMap conversion.apply
    override def unapply(value: AttributeValue): Option[T] = Option(conversion.unapply(value))
  }

  implicit object IntSet extends AttributeValueConversion[Set[Int]] {
    override val attributeType = AttributeTypes.NumberSet
    override def apply(values: Set[Int]): Option[AttributeValue] = Some(new AttributeValue().withNS(values map (_.toString)))
    override def unapply(value: AttributeValue): Set[Int] = value.getNS map (_.toInt) toSet
  }

  implicit object LongSet extends AttributeValueConversion[Set[Long]] {
    override val attributeType = AttributeTypes.NumberSet
    override def apply(values: Set[Long]): Option[AttributeValue] = Some(new AttributeValue().withNS(values map (_.toString)))
    override def unapply(value: AttributeValue): Set[Long] = value.getNS map (_.toLong) toSet
  }

  implicit object StringSet extends AttributeValueConversion[Set[String]] {
    override val attributeType = AttributeTypes.String
    override def apply(values: Set[String]): Option[AttributeValue] = Some(new AttributeValue().withSS(values))
    override def unapply(value: AttributeValue): Set[String] = value.getSS.toSet
  }

  /**
   * Conversion for dates using a lexicographically comparable format specified by ISO8601.
   * @see http://en.wikipedia.org/wiki/ISO_8601
   * @see http://www.iso.org/iso/catalogue_detail?csnumber=40874
   */
  implicit object ISO8601Date extends AttributeValueConversion[java.util.Date] {
    import java.text.SimpleDateFormat
    import java.util.{Date, TimeZone}

    // this is a `def` since SimpleDateFormat isn't thread-safe
    def format = {
      val f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      f.setTimeZone(TimeZone.getTimeZone("UTC"))
      f
    }

    override val attributeType = AttributeTypes.String
    override def apply(date:  Date) = Option(date) map { d => new AttributeValue().withS(format.format(d)) }
    override def unapply(value: AttributeValue) = {
      if (value.getS == null) null.asInstanceOf[java.util.Date] else format.parse(value.getS)
    }
  }


}