package pixii

import com.amazonaws.services.dynamodb.model._
import scala.collection._

/** Helpers for Table.update() */
object UpdateConversions {
  import AttributeAction.{ADD, DELETE, PUT}

  /** Convert a set of attribute values into `add` actions */
  def add(attributes: Map[String, AttributeValue]): Map[String, AttributeValueUpdate] = convert(attributes, ADD)

  /** `add` alias for disambiguation */
  def addAttributes(attributes: Map[String, AttributeValue]) = add(attributes)

  /** Convert a set of attribute values into `delete` actions */
  def delete(attributes: Map[String, AttributeValue]): Map[String, AttributeValueUpdate] = convert(attributes, DELETE)

  /** `delete` alias for disambiguation with Table.delete */
  def deleteAttributes(attributes: Map[String, AttributeValue]) = delete(attributes)

  /** Convert a set of attribute values into `delete` actions */
  def delete(attribute: String): Map[String, AttributeValueUpdate] = convert(attribute, DELETE)

  /** `delete` alias for disambiguation with Table.delete */
  def deleteAttribute(attribute: String) = delete(attribute)

  def delete(attributes: Seq[String]): Map[String, AttributeValueUpdate] = (attributes map delete) reduce (_ ++ _)

  /** `delete` alias for disambiguation with Table.delete */
  def deleteAttributes(attributes: Seq[String]): Map[String, AttributeValueUpdate] = delete(attributes)

  /** Convert a set of attribute values into `put` actions */
  def put(attributes: Map[String, AttributeValue]): Map[String, AttributeValueUpdate] = convert(attributes, PUT)

  /** `put` alias for disambiguation with Table.put */
  def putAttributes(attributes: Map[String, AttributeValue]) = put(attributes)

  /** Convert a set of attribute values into `add` actions */
  private def convert(attributes: Map[String, AttributeValue], action: AttributeAction): Map[String, AttributeValueUpdate] = {
    attributes mapValues { value =>
      (new AttributeValueUpdate()
        .withAction(action)
        .withValue(value))
    }
  }

  /** Convert an attribute name into an action. e.g., delete an attribute regarless of its value. */
  private def convert(attribute: String, action: AttributeAction): Map[String, AttributeValueUpdate] = {
    Map(attribute -> (new AttributeValueUpdate().withAction(action)))
  }

}
