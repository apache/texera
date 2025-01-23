package edu.uci.ics.amber.operator.intersect

import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
class IntersectOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var opExec: IntersectOpExec = _
  var counter: Int = 0

  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

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
      opExec.processTuple(commonTuples(i), input1)
    })
    assert(opExec.onFinish(input1).isEmpty)

    (5 to 9).map(i => {
      opExec.processTuple(commonTuples(i), input2)
    })

    val outputTuples: Set[TupleLike] =
      opExec.onFinish(input2).toSet
    assert(outputTuples.equals(commonTuples.slice(5, 8).toSet))

    opExec.close()
  }

  it should "work with one empty input upstream after a data stream" in {
    val input0 = 0
    val input1 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTuple(tuple(), input0)
      opExec.processTuple(commonTuples(Random.nextInt(commonTuples.size)), input0)
    })
    assert(opExec.onFinish(input0).isEmpty)

    assert(opExec.onFinish(input1).isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream after a data stream - other order" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTuple(tuple(), input1)
      opExec.processTuple(commonTuples(Random.nextInt(commonTuples.size)), input1)
    })
    assert(opExec.onFinish(input2).isEmpty)

    assert(opExec.onFinish(input1).isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream before a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    assert(opExec.onFinish(input2).isEmpty)
    (1 to 100).map(_ => {
      opExec.processTuple(tuple(), input1)
      opExec.processTuple(commonTuples(Random.nextInt(commonTuples.size)), input1)
    })
    assert(opExec.onFinish(input1).isEmpty)

    opExec.close()
  }

  it should "work with one empty input upstream during a data stream" in {
    val input1 = 0
    val input2 = 1
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTuple(tuple(), input1)
      opExec.processTuple(commonTuples(Random.nextInt(commonTuples.size)), input1)
    })
    assert(opExec.onFinish(input2).isEmpty)

    (1 to 100).map(_ => {
      opExec.processTuple(tuple(), input1)
      opExec.processTuple(commonTuples(Random.nextInt(commonTuples.size)), input1)
    })
    assert(opExec.onFinish(input1).isEmpty)

    opExec.close()
  }

  it should "work with two empty input upstreams" in {

    opExec.open()
    assert(opExec.onFinish(0).isEmpty)
    assert(opExec.onFinish(1).isEmpty)
    opExec.close()
  }

}
