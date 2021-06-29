package edu.uci.ics.texera.unittest.workflow.operators.different

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.operators.different.DifferentOpExec

class DifferentOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tuple: () => Tuple = () =>
    Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .build()

  val tuple2: () => Tuple = () =>
    Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .build()

  val tuple3: () => Tuple = () =>
    Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 2)
      .build()

  var opExec: DifferentOpExec = _
  var leftTableIdentity: LinkIdentity = LinkIdentity(
    new LayerIdentity("workflow a1", "operator b1", "layer c1"),
    new LayerIdentity("workflow d1", "operator e1", "layer f1")
  )
  var rightTableIdentity: LinkIdentity = LinkIdentity(
    new LayerIdentity("workflow a2", "operator b2", "layer c2"),
    new LayerIdentity("workflow d2", "operator e2", "layer f2")
  )
  before {
    opExec = new DifferentOpExec(rightTableIdentity)
  }

  it should "open" in {

    opExec.open()

  }

  it should "find the set different of two inputs" in {

    opExec.open()
    opExec.processTexeraTuple(Left(tuple()), rightTableIdentity)
    opExec.processTexeraTuple(Right(InputExhausted()), rightTableIdentity)
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple2()), leftTableIdentity)
    })
    (1 to 1000).map(_ => {
      opExec.processTexeraTuple(Left(tuple3()), leftTableIdentity)
    })
    val outputTuples: List[Tuple] =
      opExec.processTexeraTuple(Right(InputExhausted()), leftTableIdentity).toList
    assert(outputTuples.size == 1)
    assert(outputTuples.head.equals(tuple3()))
    opExec.close()
  }

}
