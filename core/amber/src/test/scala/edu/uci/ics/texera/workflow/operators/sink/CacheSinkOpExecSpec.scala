package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class CacheSinkOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var linkID1: LinkIdentity = _
  var dest: mutable.MutableList[Tuple] = _
  var opExec: CacheSinkOpExec = _

  def layerID(): LayerIdentity = {
    LayerIdentity("", "", "")
  }

  def linkID(): LinkIdentity = LinkIdentity(layerID(), layerID())

  before {
    linkID1 = linkID()
    dest = new mutable.MutableList[Tuple]
    opExec = new CacheSinkOpExec(dest)
  }

  it should "return empty iterator, and save nothing to dest" in {
    val iterator = opExec.processTuple(Right(InputExhausted()), linkID1)
    assert(iterator.isEmpty)
    assert(0.equals(dest.size))
  }

  it should "return empty iterator, but save right tuples to dest" in {
    val tuple1 = Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .build()
    val tuple2 = Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "world")
      .add(new Attribute("field2", AttributeType.INTEGER), 2)
      .build()
    var iterator = opExec.processTuple(Left(tuple1), linkID1)
    assert(iterator.isEmpty)
    iterator = opExec.processTuple(Left(tuple2), linkID1)
    assert(iterator.isEmpty)
    iterator = opExec.processTuple(Right(InputExhausted()), linkID1)
    assert(iterator.isEmpty)
    assert(2.equals(dest.size))
    assert(tuple1.equals(dest.head))
    assert(tuple2.equals(dest(1)))
  }

  it should "throw exception" in {
    assertThrows[AssertionError] {
      opExec = new CacheSinkOpExec(null)
    }
  }
}
