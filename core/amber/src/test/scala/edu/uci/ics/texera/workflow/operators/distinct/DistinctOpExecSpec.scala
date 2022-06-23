package edu.uci.ics.texera.workflow.operators.distinct

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class DistinctOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))
    .build()

  val tuple: () => Tuple = () =>
    Tuple
      .newBuilder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(
        new Attribute("field3", AttributeType.BOOLEAN),
        true
      )
      .build()

  val tuple2: () => Tuple = () =>
    Tuple
      .newBuilder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 2)
      .add(
        new Attribute("field3", AttributeType.BOOLEAN),
        false
      )
      .build()

  var opExec: DistinctOpExec = _
  before {
    opExec = new DistinctOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "remove duplicate Tuple with the same content" in {

    opExec.open()
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), null, null, null)
    })

    val outputTuples: List[Tuple] =
      opExec.processTexeraTuple(Right(InputExhausted()), null, null, null).toList
    assert(outputTuples.size == 1)
    assert(outputTuples.head.equals(tuple()))
    opExec.close()
  }

  it should "preserve the insertion order" in {

    opExec.open()
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), null, null, null)
    })
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple2()), null, null, null)
    })
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), null, null, null)
    })

    val outputTuples: List[Tuple] =
      opExec.processTexeraTuple(Right(InputExhausted()), null, null, null).toList
    assert(outputTuples.size == 2)
    assert(outputTuples.head.equals(tuple()))
    assert(outputTuples.apply(1).equals(tuple2()))
    opExec.close()
  }

}
