package edu.uci.ics.texera.workflow.operators.typecasting

import edu.uci.ics.amber.engine.common.tuple.amber.SchemaEnforceable
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class TypeCastingOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .builder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))
    .add(new Attribute("field4", AttributeType.LONG))
    .build()

  val castToSchema: Schema = Schema
    .builder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.STRING))
    .add(new Attribute("field3", AttributeType.STRING))
    .add(new Attribute("field4", AttributeType.LONG))
    .build()
  val castingUnit1 = new TypeCastingUnit()
  castingUnit1.attribute = "field2"
  castingUnit1.resultType = AttributeType.STRING
  val castingUnit2 = new TypeCastingUnit()
  castingUnit2.attribute = "field3"
  castingUnit2.resultType = AttributeType.STRING
  val castingUnits: java.util.List[TypeCastingUnit] = new java.util.ArrayList()
  castingUnits.add(castingUnit1)
  castingUnits.add(castingUnit2)

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .add(
      new Attribute("field4", AttributeType.LONG),
      3L
    )
    .build()

  it should "open" in {
    val typeCastingOpExec = new TypeCastingOpExec(castingUnits)
    typeCastingOpExec.open()

  }

  it should "process Tuple" in {

    val typeCastingOpExec = new TypeCastingOpExec(castingUnits)

    typeCastingOpExec.open()

    val outputTuple =
      typeCastingOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(castToSchema)

    assert(outputTuple.length == 4)
    assert(outputTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("field2").asInstanceOf[String] == "1")
    assert(outputTuple.getField("field3").asInstanceOf[String] == "true")
    assert(outputTuple.getField("field4").asInstanceOf[Long] == 3L)
    assert("hello" == outputTuple.getField[String](0))
    assert(outputTuple.getField[String](1) == "1")
    assert(outputTuple.getField[String](2) == "true")
    assert(outputTuple.getField[Long](3) == 3L)
  }
}
