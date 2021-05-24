package edu.uci.ics.texera.unittest.workflow.operators.distinct

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.distinct.DistinctOpExec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
class DistinctOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tuple1: Tuple = Tuple
    .newBuilder()
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()

  val tuple1Dup: Tuple = Tuple.newBuilder().add(tuple1).build()

  var opExec: DistinctOpExec = _
  before {
    opExec = new DistinctOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "remove duplicate Tuple with the same reference" in {

    opExec.open()
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple1), null)
    })
    val outputTuples: List[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), null).toList
    assert(outputTuples.size == 1)
    assert(outputTuples.head.equals(tuple1))
  }

  it should "remove duplicate Tuple with the same content" in {

    opExec.open()
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple1), null)
      opExec.processTexeraTuple(Left(tuple1Dup), null)
    })

    val outputTuples: List[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), null).toList
    assert(outputTuples.size == 1)
    assert(outputTuples.head.equals(tuple1))
  }

}
