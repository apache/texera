package edu.uci.ics.texera.workflow.operators.difference

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.{SchemaEnforceable, TupleLike}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random

class DifferenceOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var input1: Int = 0
  var input2: Int = 1
  var opExec: DifferenceOpExec = _
  var counter: Int = 0
  val schema: Schema = Schema
    .builder()
    .add(
      new Attribute("field1", AttributeType.STRING),
      new Attribute("field2", AttributeType.INTEGER),
      new Attribute("field3", AttributeType.BOOLEAN)
    )
    .build()

  def tuple(): Tuple = {
    counter += 1

    Tuple
      .builder(schema)
      .addSequentially(Array("hello", Int.box(counter), Boolean.box(true)))
      .build()
  }

  before {
    opExec = new DifferenceOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "work with basic two input streams with no duplicates" in {
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
    assert(
      outputTuples.equals(commonTuples.slice(0, 5).toSet)
    )

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
            .enforceSchema(schema)
            .getField[Int]("field2") <= 10
        )
      )
      opExec.close()
    }
  }

  it should "work with one empty input upstream after a data stream" in {
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 9).map(i => {
      opExec.processTuple(Left(commonTuples(i)), input1)
    })
    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)

    val outputTuples: Set[TupleLike] =
      opExec.processTuple(Right(InputExhausted()), input2).toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with one empty input upstream after a data stream - other order" in {
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 9).map(i => {
      opExec.processTuple(Left(commonTuples(i)), input2)
    })
    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)

    val outputTuples: Set[TupleLike] =
      opExec.processTuple(Right(InputExhausted()), input1).toSet
    assert(outputTuples.isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream before a data stream" in {
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)
    (0 to 9).map(i => {
      opExec.processTuple(Left(commonTuples(i)), input1)
    })

    val outputTuples: Set[TupleLike] =
      opExec.processTuple(Right(InputExhausted()), input1).toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with one empty input upstream during a data stream" in {
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 5).map(i => {
      opExec.processTuple(Left(commonTuples(i)), input1)
    })
    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)
    (6 to 9).map(i => {
      opExec.processTuple(Left(commonTuples(i)), input1)
    })

    val outputTuples: Set[TupleLike] =
      opExec.processTuple(Right(InputExhausted()), input1).toSet
    assert(outputTuples.equals(commonTuples.toSet))
    opExec.close()
  }

  it should "work with two empty input upstreams" in {

    opExec.open()
    assert(opExec.processTuple(Right(InputExhausted()), input1).isEmpty)
    assert(opExec.processTuple(Right(InputExhausted()), input2).isEmpty)
    opExec.close()
  }

}
