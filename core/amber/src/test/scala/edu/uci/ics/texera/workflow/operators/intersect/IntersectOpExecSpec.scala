package edu.uci.ics.texera.workflow.operators.intersect

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.{SchemaEnforceable, TupleLike}
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random
class IntersectOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var opExec: IntersectOpExec = _
  var counter: Int = 0

  val tupleSchema: Schema = Schema
    .builder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(
      new Attribute("field3", AttributeType.BOOLEAN)
    )
    .build()

  def physicalOpId(): PhysicalOpIdentity = {
    counter += 1
    PhysicalOpIdentity(OperatorIdentity("" + counter), "" + counter)
  }

  def physicalLink(): PhysicalLink =
    PhysicalLink(
      physicalOpId(),
      fromPortId = PortIdentity(),
      physicalOpId(),
      toPortId = PortIdentity()
    )

  def tuple(): Tuple = {
    counter += 1
    Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), counter)
      .add(
        new Attribute("field3", AttributeType.BOOLEAN),
        true
      )
      .build()
  }

  before {
    opExec = new IntersectOpExec()
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
      opExec.processTuple(Left(commonTuples(i)), input1)
    })
    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)

    (5 to 9).map(i => {
      opExec.processTuple(Left(commonTuples(i)), input2)
    })

    val outputTuples: Set[TupleLike] =
      opExec.processTuple(Right(InputExhausted()), input2).toSet
    assert(outputTuples.equals(commonTuples.slice(5, 8).toSet))

    opExec.close()
  }

  it should "raise IllegalArgumentException when intersect with more than two input upstreams" in {

    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList
    assertThrows[IllegalArgumentException] {
      (1 to 100).map(_ => {
        opExec.processTuple(Left(tuple()), 2)
        opExec.processTuple(
          Left(commonTuples(Random.nextInt(commonTuples.size))),
          3
        )
      })

      val outputTuples: Set[TupleLike] =
        opExec.processTuple(Right(InputExhausted()), 0).toSet
      assert(outputTuples.size <= 10)
      assert(outputTuples.subsetOf(commonTuples.toSet))
      outputTuples.foreach(tupleLike =>
        assert(
          tupleLike
            .asInstanceOf[SchemaEnforceable]
            .enforceSchema(tupleSchema)
            .getField[Int]("field2") <= 10
        )
      )
      opExec.close()
    }
  }

  it should "work with one empty input upstream after a data stream" in {
    val input0 = 0
    val input1 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTuple(Left(tuple()), input0)
      opExec.processTuple(
        Left(commonTuples(Random.nextInt(commonTuples.size))),
        input0
      )
    })
    assert(opExec.processTuple(Right(InputExhausted()), input0).isEmpty)

    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream after a data stream - other order" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTuple(Left(tuple()), input1)
      opExec.processTuple(
        Left(commonTuples(Random.nextInt(commonTuples.size))),
        input1
      )
    })
    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)

    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream before a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)
    (1 to 100).map(_ => {
      opExec.processTuple(Left(tuple()), input1)
      opExec.processTuple(
        Left(commonTuples(Random.nextInt(commonTuples.size))),
        input1
      )
    })
    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)

    opExec.close()
  }

  it should "work with one empty input upstream during a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTuple(Left(tuple()), input1)
      opExec.processTuple(
        Left(commonTuples(Random.nextInt(commonTuples.size))),
        input1
      )
    })
    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)

    (1 to 100).map(_ => {
      opExec.processTuple(Left(tuple()), input1)
      opExec.processTuple(
        Left(commonTuples(Random.nextInt(commonTuples.size))),
        input1
      )
    })
    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)

    opExec.close()
  }

  it should "work with two empty input upstreams" in {

    opExec.open()
    assert(opExec.processTuple(Right(InputExhausted()), 0).isEmpty)
    assert(opExec.processTuple(Right(InputExhausted()), 1).isEmpty)
    opExec.close()
  }

}
