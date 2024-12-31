package edu.uci.ics.amber.operator.symmetricDifference

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple,
  TupleLike
}
class SymmetricDifferenceOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var opExec: SymmetricDifferenceOpExec = _
  var counter: Int = 0
  val schema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  def tuple(): Tuple = {
    counter += 1

    Tuple
      .builder(schema)
      .addSequentially(Array("hello", Int.box(counter), Boolean.box(true)))
      .build()
  }

  before {
    opExec = new SymmetricDifferenceOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "work with basic two input streams with no duplicates" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 7).map(i => {
      opExec.processTuple(commonTuples(i), input1)
    })
    assert(opExec.onFinish(input1).isEmpty)

    (5 to 9).map(i => {
      opExec.processTuple(commonTuples(i), input2)
    })

    val outputTuples: Set[TupleLike] =
      opExec.onFinish(input2).toSet
    assert(
      outputTuples.equals(commonTuples.slice(0, 5).toSet.union(commonTuples.slice(8, 10).toSet))
    )

    opExec.close()
  }

  it should "work with one empty input upstream after a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 9).map(i => {
      opExec.processTuple(commonTuples(i), input1)
    })
    assert(opExec.onFinish(input1).isEmpty)

    val outputTuples: Set[Tuple] =
      opExec
        .onFinish(input2)
        .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(schema))
        .toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with one empty input upstream after a data stream - other order" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 9).map(i => {
      opExec.processTuple(commonTuples(i), input2)
    })
    assert(opExec.onFinish(input2).isEmpty)

    val outputTuples: Set[Tuple] =
      opExec
        .onFinish(input1)
        .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(schema))
        .toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with one empty input upstream before a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    assert(opExec.onFinish(input2).isEmpty)
    (0 to 9).map(i => {
      opExec.processTuple(commonTuples(i), input1)
    })

    val outputTuples: Set[Tuple] =
      opExec
        .onFinish(input1)
        .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(schema))
        .toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with one empty input upstream during a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 5).map(i => {
      opExec.processTuple(commonTuples(i), input1)
    })
    assert(opExec.onFinish(input2).isEmpty)
    (6 to 9).map(i => {
      opExec.processTuple(commonTuples(i), input1)
    })

    val outputTuples: Set[Tuple] =
      opExec
        .onFinish(input1)
        .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(schema))
        .toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with two empty input upstreams" in {

    opExec.open()
    assert(opExec.onFinish(0).isEmpty)
    assert(opExec.onFinish(1).isEmpty)
    opExec.close()
  }

}
