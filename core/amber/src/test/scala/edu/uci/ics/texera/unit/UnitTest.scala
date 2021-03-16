package edu.uci.ics.texera.unit
import edu.uci.ics.texera.workflow.common.AttributeTypeUtils
import org.scalatest.funsuite.AnyFunSuite
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._

class UnitTest extends AnyFunSuite {
  // Unit Test for Infer Schema
  test("infer String should get AttributeType.STRING") {
    val string: String = "string"
    assert(AttributeTypeUtils.inferField(string) === STRING)
  }
  test("infer Integer should get AttributeType.INTEGER") {
    val integer: String = "1"
    assert(AttributeTypeUtils.inferField(integer) === INTEGER)
  }
  test("infer Double should get AttributeType.DOUBLE") {
    val double: String = "4.0"
    assert(AttributeTypeUtils.inferField(double) === DOUBLE)
  }
  test("infer Boolean should get AttributeType.Boolean") {
    val double: String = "true"
    assert(AttributeTypeUtils.inferField(double) === BOOLEAN)
  }
  test("infer Long should get AttributeType.LONG") {
    val double: String = "2147483648"
    assert(AttributeTypeUtils.inferField(double) === LONG)
  }

}
