package edu.uci.ics.texera.workflow.operators.projection

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class ProjectionOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))
    .build()

  val tuple: Tuple = Tuple
    .newBuilder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()

  it should "open" in {
    val projectionOpExec = new ProjectionOpExec(List(), null)
    projectionOpExec.attributes ++= List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    projectionOpExec.open()

  }

  it should "process Tuple" in {
    val outputSchema = Schema
      .newBuilder()
      .add(new Attribute("f1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))
      .build()
    val projectionOpExec =
      new ProjectionOpExec(List(), OperatorSchemaInfo(null, Array(outputSchema)))
    projectionOpExec.attributes ++= List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )

    projectionOpExec.open()

    val processedTuple = projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.length() == 2)
    assert(processedTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(processedTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(processedTuple.get(0) == "hello")
    assert(processedTuple.get(1) == 1)
  }

  it should "process Tuple with different order" in {
    val outputSchema = Schema
      .newBuilder()
      .add(new Attribute("f3", AttributeType.BOOLEAN))
      .add(new Attribute("f1", AttributeType.STRING))
      .build()
    val projectionOpExec =
      new ProjectionOpExec(List(), OperatorSchemaInfo(null, Array(outputSchema)))
    projectionOpExec.attributes ++= List(
      new AttributeUnit("field3", "f3"),
      new AttributeUnit("field1", "f1")
    )

    projectionOpExec.open()

    val processedTuple = projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.length() == 2)
    assert(processedTuple.getField("f3").asInstanceOf[Boolean])
    assert(processedTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(processedTuple.get(0) == true)
    assert(processedTuple.get(1) == "hello")
  }

  it should "raise RuntimeException on non-existing fields" in {
    val projectionOpExec = new ProjectionOpExec(List(), null)
    projectionOpExec.attributes ++= List(
      new AttributeUnit("field---5", "f5"),
      new AttributeUnit("field---6", "f6")
    )
    assertThrows[RuntimeException] {
      projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    }

  }

  it should "raise IllegalArgumentException on empty attributes" in {
    val projectionOpExec = new ProjectionOpExec(List(), null)
    assertThrows[IllegalArgumentException] {
      projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    }

  }

  it should "raise RuntimeException on duplicate alias" in {
    val projectionOpExec = new ProjectionOpExec(List(), null)
    projectionOpExec.attributes ++= List(
      new AttributeUnit("field1", "f"),
      new AttributeUnit("field2", "f")
    )
    assertThrows[RuntimeException] {
      projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    }

  }

  it should "allow empty alias" in {
    val outputSchema = Schema
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))
      .build()
    val projectionOpExec =
      new ProjectionOpExec(List(), OperatorSchemaInfo(null, Array(outputSchema)))
    projectionOpExec.attributes ++= List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "")
    )

    projectionOpExec.open()

    val processedTuple = projectionOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.length() == 2)
    assert(processedTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(processedTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(processedTuple.get(0) == "hello")
    assert(processedTuple.get(1) == 1)
  }

}
