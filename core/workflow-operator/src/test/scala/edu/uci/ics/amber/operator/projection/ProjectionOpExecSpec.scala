package edu.uci.ics.amber.operator.projection

import edu.uci.ics.amber.core.tuple._
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class ProjectionOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()
  val opDesc: ProjectionOpDesc = new ProjectionOpDesc()

  it should "open" in {
    opDesc.attributes = List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

  }

  it should "process Tuple" in {
    opDesc.attributes = List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    val outputSchema = Schema()
      .add(new Attribute("f1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
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
    opDesc.attributes = List(
      new AttributeUnit("field3", "f3"),
      new AttributeUnit("field1", "f1")
    )
    val outputSchema = Schema()
      .add(new Attribute("f3", AttributeType.BOOLEAN))
      .add(new Attribute("f1", AttributeType.STRING))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
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

  it should "meException on non-existing fields" in {
    opDesc.attributes = List(
      new AttributeUnit("field---5", "f5"),
      new AttributeUnit("field---6", "f6")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    assertThrows[RuntimeException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }
  }

  it should "raise IllegalArgumentException on empty attributes" in {
    opDesc.attributes = List()
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    assertThrows[IllegalArgumentException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }
  }

  it should "raise RuntimeException on duplicate alias" in {
    opDesc.attributes = List(
      new AttributeUnit("field1", "f"),
      new AttributeUnit("field2", "f")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    assertThrows[RuntimeException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }
  }

  it should "allow empty alias" in {
    opDesc.attributes = List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "")
    )
    val outputSchema = Schema()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
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
