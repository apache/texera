package edu.uci.ics.texera.workflow.operators.filter

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Arrays.asList

class SpecializedFilterOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val inputPort: Int = 0

  val tuplesWithOneFieldNull: Iterable[Tuple] =
    AttributeType
      .values()
      .map(attributeType =>
        Tuple
          .builder(
            Schema.builder().add(new Attribute(attributeType.name(), attributeType)).build()
          )
          .add(new Attribute(attributeType.name(), attributeType), null)
          .build()
      )

  val tupleSchema: Schema = Schema
    .builder()
    .add(new Attribute("string", AttributeType.STRING))
    .add(new Attribute("int", AttributeType.INTEGER))
    .add(new Attribute("bool", AttributeType.BOOLEAN))
    .add(new Attribute("long", AttributeType.LONG))
    .build()

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
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc())
    opExec.open()
    opExec.close()
  }

  it should "throw when predicates is null" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc())
    opExec.open()
    assertThrows[NullPointerException] {
      opExec.processTuple(Left(allNullTuple), inputPort)
    }
    opExec.close()
  }

  it should "do nothing when predicates is an empty list" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
      predicates = asList()
    })
    opExec.open()
    assert(opExec.processTuple(Left(allNullTuple), inputPort).isEmpty)
    opExec.close()
  }

  it should "not have is_null comparisons be affected by values" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
      predicates = asList(new FilterPredicate("string", ComparisonType.IS_NULL, "value"))
    })
    opExec.open()
    assert(!opExec.processTuple(Left(allNullTuple), inputPort).isEmpty)
    opExec.close()
  }

  it should "not have is_not_null comparisons be affected by values" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
      predicates = asList(new FilterPredicate("string", ComparisonType.IS_NOT_NULL, "value"))
    })
    opExec.open()
    assert(opExec.processTuple(Left(allNullTuple), inputPort).isEmpty)
    opExec.close()
  }

  it should "output null tuples when filtering is_null" in {
    tuplesWithOneFieldNull
      .map(nullTuple => {
        val attributes = nullTuple.getSchema.getAttributes
        assert(attributes.length == 1)

        val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
          predicates =
            asList(new FilterPredicate(attributes(0).getName, ComparisonType.IS_NULL, null))
        })

        opExec.open()
        assert(opExec.processTuple(Left(nullTuple), inputPort).nonEmpty)
        opExec.close()
      })
  }

  it should "filter out non null tuples when filtering is_null" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
      predicates = asList(new FilterPredicate("string", ComparisonType.IS_NULL, "value"))
    })
    opExec.open()
    assert(opExec.processTuple(Left(nonNullTuple), inputPort).isEmpty)
    opExec.close()
  }

  it should "output non null tuples when filter is_not_null" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
      predicates = asList(new FilterPredicate("string", ComparisonType.IS_NOT_NULL, "value"))
    })
    opExec.open()
    assert(opExec.processTuple(Left(nonNullTuple), inputPort).nonEmpty)
    opExec.close()
  }

  it should "filter out null tuples when filter is_not_null" in {
    tuplesWithOneFieldNull
      .map(nullTuple => {
        val attributes = nullTuple.getSchema.getAttributes
        assert(attributes.length == 1)

        val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc() {
          predicates = asList(
            new FilterPredicate(attributes(0).getName, ComparisonType.IS_NOT_NULL, null)
          )
        })

        opExec.open()
        assert(opExec.processTuple(Left(nullTuple), inputPort).isEmpty)
        opExec.close()
      })
  }
}
