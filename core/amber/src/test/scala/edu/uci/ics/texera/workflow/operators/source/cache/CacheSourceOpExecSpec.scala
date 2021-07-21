package edu.uci.ics.texera.workflow.operators.source.cache

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class CacheSourceOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var linkID1: LinkIdentity = _
  var src: mutable.MutableList[Tuple] = _
  var opExec: CacheSourceOpExec = _

  def layerID(): LayerIdentity = {
    LayerIdentity("", "", "")
  }

  def linkID(): LinkIdentity = LinkIdentity(layerID(), layerID())

  before {
    linkID1 = linkID()
    src = new mutable.MutableList[Tuple]
    opExec = new CacheSourceOpExec(src)
  }

  it should "return empty iterator" in {
    val iterator = opExec.processTuple(Right(InputExhausted()), linkID1)
    assert(iterator.isEmpty)
  }

  it should "return iterator with correct tuples" in {
    src += Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .build()
    src += Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "world")
      .add(new Attribute("field2", AttributeType.INTEGER), 2)
      .build()
    opExec = new CacheSourceOpExec(src)
    val iterator = opExec.produceTexeraTuple()
    assert(src.head.equals(iterator.next()))
    assert(src(1).equals(iterator.next()))
  }

  it should "throw exception" in {
    assertThrows[AssertionError] {
      opExec = new CacheSourceOpExec(null)
    }
  }
}
