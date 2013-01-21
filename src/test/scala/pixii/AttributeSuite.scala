package pixii

import pixii.AttributeModifiers._
import pixii.AttributeValueConversions._

import com.amazonaws.services.dynamodb.model.AttributeValue
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AttributeSuite extends WordSpec with ShouldMatchers {

  val emptyAttrs = Map[String, AttributeValue]()

  "NamedAttribute" should {
    val named = new NamedAttribute[String]("foo") with Nullable

    "have name" in {
      named.name should be === "foo"
    }

    "have AttributeType corresponding to type argument" in {
      named.conversion.attributeType should be === AttributeTypes.String
    }
  }

  "Required mixing trait" should {
    val required = new NamedAttribute[String]("foo") with Required

    "return value when attribute is present" in {
      val attrs = Map("foo" -> new AttributeValue().withS("bar"))
      required.get(attrs) should be === "bar"
    }

    "raise an exception when attribute is absent" in {
      evaluating { required.get(emptyAttrs) } should produce [MissingAttributeException]
    }

    "produce an attribute when passed a value" in {
      required.apply("bar") should be === Map("foo" -> new AttributeValue().withS("bar"))
    }

    "raise an exception with attribute value is null" in {
      evaluating { required.apply(null) } should produce[RequiredAttributeCannotBeNullException]
    }
  }

  "Nullable mixin trait" should {
    val nullable = new NamedAttribute[String]("foo") with Nullable

    "return value when attribute is present" in {
      val attrs = Map("foo" -> new AttributeValue().withS("bar"))
      nullable.get(attrs) should be === "bar"
    }

    "return `null` when attribute is absent" in {
      nullable.get(emptyAttrs) should be === null
    }

    "produce an attribute when passed a value" in {
      nullable.apply("bar") should be === Map("foo" -> new AttributeValue().withS("bar"))
    }

    "not produce an attribute when passed `null`" in {
      nullable.apply(null) should be === Map.empty
    }
  }

  "Default mixing trait" should {
    val withDefault = new NamedAttribute[String]("foo") with Default {
      override val defaultValue = "default"
    }

    "return the default value" in {
      withDefault.defaultValue should be === "default"
    }

    "return value when attribute is present" in {
      val attrs = Map("foo" -> new AttributeValue().withS("bar"))
      withDefault.get(attrs) should be === "bar"
    }

    "return default value when attribute is absent" in {
      withDefault.get(emptyAttrs) should be === "default"
    }

    "produce an attribute when passed a value" in {
      withDefault.apply("bar") should be === Map("foo" -> new AttributeValue().withS("bar"))
    }

    "produce an attribute with default value when value is null" in {
      withDefault.apply(null) should be === Map("foo" -> new AttributeValue().withS("default"))
    }
  }

  "SafeDefault mixin trait" should {
    val withSafeDefault = new NamedAttribute[Int]("foo") with SafeDefault with Required  {
      override val safeValue = Some(42)
    }

    "return the safe value" in {
      withSafeDefault.safeValue should be === Some(42)
    }

    "return value when attribute is valid" in {
      val attrs = Map("foo" -> new AttributeValue().withN("777"))
      withSafeDefault.get(attrs) should be === 777
    }

    "return safe value when attribute is invalid" in {
      val attrs = Map("foo" -> new AttributeValue().withN("bad"))
      withSafeDefault.get(attrs) should be === 42
    }
  }
}