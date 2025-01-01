package edu.uci.ics.amber.operator.filter

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class SpecializedFilterOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val inputPort: Int = 0
  val opDesc: SpecializedFilterOpDesc = new SpecializedFilterOpDesc()
  val tuplesWithOneFieldNull: Iterable[Tuple] =
    AttributeType
      .values()
      .map(attributeType =>
        Tuple
          .builder(
            Schema().add(new Attribute(attributeType.name(), attributeType))
          )
          .add(new Attribute(attributeType.name(), attributeType), null)
          .build()
      )

  val tupleSchema: Schema = Schema()
    .add(new Attribute("string", AttributeType.STRING))
    .add(new Attribute("int", AttributeType.INTEGER))
    .add(new Attribute("bool", AttributeType.BOOLEAN))
    .add(new Attribute("long", AttributeType.LONG))

  val allNullTuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("string", AttributeType.STRING), null)
    .add(new Attribute("int", AttributeType.INTEGER), null)
    .add(new Attribute("bool", AttributeType.BOOLEAN), null)
    .add(new Attribute("long", AttributeType.LONG), null)
    .build()

  val nonNullTuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("string", AttributeType.STRING), "hello")
    .add(new Attribute("int", AttributeType.INTEGER), 0)
    .add(new Attribute("bool", AttributeType.BOOLEAN), false)
    .add(new Attribute("long", AttributeType.LONG), Long.MaxValue)
    .build()

  it should "open and close" in {
    opDesc.predicates = List()
    val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    opExec.close()
  }

  it should "do nothing when predicates is an empty list" in {
    opDesc.predicates = List()
    val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.processTuple(allNullTuple, inputPort).isEmpty)
    opExec.close()
  }

  it should "not have is_null comparisons be affected by values" in {
    opDesc.predicates = List(new FilterPredicate("string", ComparisonType.IS_NULL, "value"))
    val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.processTuple(allNullTuple, inputPort).nonEmpty)
    opExec.close()
  }

  it should "not have is_not_null comparisons be affected by values" in {
    opDesc.predicates = List(new FilterPredicate("string", ComparisonType.IS_NOT_NULL, "value"))
    val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.processTuple(allNullTuple, inputPort).isEmpty)
    opExec.close()
  }

  it should "output null tuples when filtering is_null" in {
    tuplesWithOneFieldNull
      .map(nullTuple => {
        val attributes = nullTuple.getSchema.getAttributes
        assert(attributes.length == 1)
        opDesc.predicates =
          List(new FilterPredicate(attributes.head.getName, ComparisonType.IS_NULL, null))
        val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
        opExec.open()
        assert(opExec.processTuple(nullTuple, inputPort).nonEmpty)
        opExec.close()
      })
  }

  it should "filter out non null tuples when filtering is_null" in {
    opDesc.predicates = List(new FilterPredicate("string", ComparisonType.IS_NULL, "value"))
    val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.processTuple(nonNullTuple, inputPort).isEmpty)
    opExec.close()
  }

  it should "output non null tuples when filter is_not_null" in {
    opDesc.predicates = List(new FilterPredicate("string", ComparisonType.IS_NOT_NULL, "value"))
    val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.processTuple(nonNullTuple, inputPort).nonEmpty)
    opExec.close()
  }

  it should "filter out null tuples when filter is_not_null" in {
    tuplesWithOneFieldNull
      .map(nullTuple => {
        val attributes = nullTuple.getSchema.getAttributes
        assert(attributes.length == 1)
        opDesc.predicates =
          List(new FilterPredicate(attributes.head.getName, ComparisonType.IS_NOT_NULL, null))
        val opExec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(opDesc))
        opExec.open()
        assert(opExec.processTuple(nullTuple, inputPort).isEmpty)
        opExec.close()
      })
  }
}
