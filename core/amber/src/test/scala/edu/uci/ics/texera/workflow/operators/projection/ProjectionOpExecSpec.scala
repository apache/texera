package edu.uci.ics.texera.workflow.operators.projection

import edu.uci.ics.amber.engine.common.tuple.amber.SchemaEnforceable
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class ProjectionOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .builder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))
    .build()

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()

  it should "open" in {
    val projectionOpExec = new ProjectionOpExec(
      List(
        new AttributeUnit("field2", "f2"),
        new AttributeUnit("field1", "f1")
      )
    )
    projectionOpExec.open()

  }

  it should "process Tuple" in {
    val outputSchema = Schema
      .builder()
      .add(new Attribute("f1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))
      .build()
    val projectionOpExec = new ProjectionOpExec(
      List(
        new AttributeUnit("field2", "f2"),
        new AttributeUnit("field1", "f1")
      )
    )

    projectionOpExec.open()

    val outputTuple =
      projectionOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Int](1) == 1)
  }

  it should "process Tuple with different order" in {
    val outputSchema = Schema
      .builder()
      .add(new Attribute("f3", AttributeType.BOOLEAN))
      .add(new Attribute("f1", AttributeType.STRING))
      .build()
    val projectionOpExec = new ProjectionOpExec(
      List(
        new AttributeUnit("field3", "f3"),
        new AttributeUnit("field1", "f1")
      )
    )

    projectionOpExec.open()

    val outputTuple =
      projectionOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("f3").asInstanceOf[Boolean])
    assert(outputTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField[Boolean](0))
    assert(outputTuple.getField[String](1) == "hello")
  }

  it should "raise RuntimeException on non-existing fields" in {
    val projectionOpExec = new ProjectionOpExec(
      List(
        new AttributeUnit("field---5", "f5"),
        new AttributeUnit("field---6", "f6")
      )
    )
    assertThrows[RuntimeException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }

  }

  it should "raise IllegalArgumentException on empty attributes" in {
    val projectionOpExec = new ProjectionOpExec(List())
    assertThrows[IllegalArgumentException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }

  }

  it should "raise RuntimeException on duplicate alias" in {
    val projectionOpExec = new ProjectionOpExec(
      List(
        new AttributeUnit("field1", "f"),
        new AttributeUnit("field2", "f")
      )
    )

    assertThrows[RuntimeException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }

  }

  it should "allow empty alias" in {
    val outputSchema = Schema
      .builder()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))
      .build()
    val projectionOpExec = new ProjectionOpExec(
      List(
        new AttributeUnit("field2", "f2"),
        new AttributeUnit("field1", "")
      )
    )

    projectionOpExec.open()

    val outputTuple =
      projectionOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Int](1) == 1)
  }

}
