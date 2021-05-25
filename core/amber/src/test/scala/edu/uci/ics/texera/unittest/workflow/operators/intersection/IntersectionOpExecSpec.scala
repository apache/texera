package edu.uci.ics.texera.unittest.workflow.operators.intersection

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.intersection.IntersectionOpExec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter

import scala.util.Random
class IntersectionOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var opExec: IntersectionOpExec = _
  var counter: Int = 0

  def layerID(): LayerIdentity = {
    counter += 1
    LayerIdentity("" + counter, "" + counter, "" + counter)
  }

  def linkID(): LinkIdentity = LinkIdentity(layerID(), layerID())

  def tuple(): Tuple = {
    counter += 1
    Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), counter)
      .add(
        new Attribute("field3", AttributeType.BOOLEAN),
        true
      )
      .build()
  }

  before {
    opExec = new IntersectionOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "raise RuntimeException when intersect with only one input upstream" in {
    val linkID1 = linkID()
    opExec.open()
    assertThrows[IllegalArgumentException] {
      (1 to 1000).map(_ => {
        opExec.processTexeraTuple(Left(tuple()), linkID1)
      })

      opExec.processTexeraTuple(Right(InputExhausted()), null).toList
    }

    opExec.close()
  }

  it should "work with basic 2 input streams with no duplicates" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 7).map(i => {
      opExec.processTexeraTuple(Left(commonTuples(i)), linkID1)
    })

    (5 to 9).map(i => {
      opExec.processTexeraTuple(Left(commonTuples(i)), linkID2)
    })

    val outputTuples: Set[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), null).toSet
    assert(outputTuples.equals(commonTuples.slice(5, 8).toSet))

    opExec.close()
  }

  it should "work with 2 random input upstreams" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 10).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID1)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })

    (1 to 10).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID2)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })

    val outputTuples: Set[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), null).toSet
    assert(outputTuples.size <= 10)
    assert(outputTuples.subsetOf(commonTuples.toSet))
    outputTuples.foreach(tuple => assert(tuple.getField[Int]("field2") < 10))
    opExec.close()
  }

  it should "work with more than 2 random input upstreams" in {

    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID())
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID())
    })

    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID())
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID())
    })

    val outputTuples: Set[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), null).toSet
    assert(outputTuples.size <= 10)
    assert(outputTuples.subsetOf(commonTuples.toSet))
    outputTuples.foreach(tuple => assert(tuple.getField[Int]("field2") < 10))
    opExec.close()
  }

}
