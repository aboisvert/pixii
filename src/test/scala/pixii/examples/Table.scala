package pixii.examples

import pixii._
import pixii.AttributeModifiers._
import pixii.AttributeValueConversions._
import pixii.KeySchema._

import com.amazonaws.services.dynamodb.model.{AttributeValue, Key}
import scala.collection._

case class Foo(val s: String, val x: Int)

trait FooTable extends Table[Foo] with HashKeyTable[String, Foo] {
  val s = new NamedAttribute[String]("s") with Required
  val x = new NamedAttribute[Int]("x") with Required

  override val tableName = "MyTable"
  override val hashKeyAttribute = s
  override val itemMapper = new ItemMapper[Foo] {
    override def apply(f: Foo) = {
      s(f.s) ++ x(f.x)
    }
    override def unapply(item: Map[String, AttributeValue]) = {
      Foo(s.get(item), x.get(item))
    }
  }

  def updateExample(foo: Foo) = {
    update(
      key = foo.s,
      attributeUpdates = UpdateConversions.put(x(foo.x))
    )
  }
}