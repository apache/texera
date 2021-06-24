package edu.uci.ics.texera.unittest.workflow.operators.intersect

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.intersect.IntersectOpExec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter

import scala.util.Random
class IntersectOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var opExec: IntersectOpExec = _
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
    opExec = new IntersectOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "work with basic two input streams with no duplicates" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (0 to 7).map(i => {
      opExec.processTexeraTuple(Left(commonTuples(i)), linkID1)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)

    (5 to 9).map(i => {
      opExec.processTexeraTuple(Left(commonTuples(i)), linkID2)
    })

    val outputTuples: Set[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), linkID2).toSet
    assert(outputTuples.equals(commonTuples.slice(5, 8).toSet))

    opExec.close()
  }

  it should "work with two random input upstreams" in {
    val links = (0 to 1).map(_ => linkID()).toList
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 10).map(_ => {

      opExec.processTexeraTuple(Left(tuple()), links(Random.nextInt(links.size)))
      opExec.processTexeraTuple(
        Left(commonTuples(Random.nextInt(commonTuples.size))),
        links(Random.nextInt(links.size))
      )
    })

    assert(opExec.processTexeraTuple(Right(InputExhausted()), links.head).isEmpty)

    val outputTuples: Set[Tuple] =
      opExec.processTexeraTuple(Right(InputExhausted()), links(1)).toSet
    assert(outputTuples.size <= 10)
    assert(outputTuples.subsetOf(commonTuples.toSet))
    outputTuples.foreach(tuple => assert(tuple.getField[Int]("field2") <= 10))
    opExec.close()
  }

  it should "raise IllegalArgumentException when intersect with more than two input upstreams" in {

    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList
    assertThrows[IllegalArgumentException] {
      (1 to 100).map(_ => {
        opExec.processTexeraTuple(Left(tuple()), linkID())
        opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID())
      })

      val outputTuples: Set[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), null).toSet
      assert(outputTuples.size <= 10)
      assert(outputTuples.subsetOf(commonTuples.toSet))
      outputTuples.foreach(tuple => assert(tuple.getField[Int]("field2") <= 10))
      opExec.close()
    }
  }

  it should "work with one empty input upstream after a data stream" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID1)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)

    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID2).isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream after a data stream - other order" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID1)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID2).isEmpty)

    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)
    opExec.close()
  }

  it should "work with one empty input upstream before a data stream" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID2).isEmpty)
    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID1)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)

    opExec.close()
  }

  it should "work with one empty input upstream during a data stream" in {
    val linkID1 = linkID()
    val linkID2 = linkID()
    opExec.open()
    counter = 0
    val commonTuples = (1 to 10).map(_ => tuple()).toList

    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID1)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID2).isEmpty)

    (1 to 100).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID1)
      opExec.processTexeraTuple(Left(commonTuples(Random.nextInt(commonTuples.size))), linkID1)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)

    opExec.close()
  }

  it should "work with two empty input upstreams" in {

    opExec.open()
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID()).isEmpty)
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID()).isEmpty)
    opExec.close()
  }

}
