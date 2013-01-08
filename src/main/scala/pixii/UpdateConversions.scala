package pixii

import com.amazonaws.services.dynamodb.model._
import scala.collection._

/** Helpers for Table.update() */
object UpdateConversions {
  import AttributeAction.{ADD, DELETE, PUT}
  
  /** Convert a set of attribute values into `add` actions */
  def add(attributes: Map[String, AttributeValue]): Map[String, AttributeValueUpdate] = convert(attributes, ADD)

  /** `add` alias for diambiguation */
  def addAttributes(attributes: Map[String, AttributeValue]) = add(attributes)
  
  /** Convert a set of attribute values into `delete` actions */
  def delete(attributes: Map[String, AttributeValue]): Map[String, AttributeValueUpdate] = convert(attributes, DELETE)

  /** `delete` alias for diambiguation with Table.delete */
  def deleteAttributes(attributes: Map[String, AttributeValue]) = delete(attributes)
  
  /** Convert a set of attribute values into `put` actions */
  def put(attributes: Map[String, AttributeValue]): Map[String, AttributeValueUpdate] = convert(attributes, PUT)

  /** `put` alias for diambiguation with Table.put */
  def putAttributes(attributes: Map[String, AttributeValue]) = put(attributes)
  
  /** Convert a set of attribute values into `add` actions */
  private def convert(attributes: Map[String, AttributeValue], action: AttributeAction): Map[String, AttributeValueUpdate] = {
    attributes mapValues { value => 
      (new AttributeValueUpdate()
        .withAction(action)
        .withValue(value))
    }
  }
}
