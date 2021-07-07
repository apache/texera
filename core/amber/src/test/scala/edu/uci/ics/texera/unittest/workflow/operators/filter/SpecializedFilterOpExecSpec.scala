package edu.uci.ics.texera.unittest.workflow.operators.filter

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.filter.{ComparisonType,FilterPredicate, SpecializedFilterOpDesc, SpecializedFilterOpExec}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.util
//test sample for filter operator
class SpecializedFilterOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: SpecializedFilterOpDesc = _
  var opExec: SpecializedFilterOpExec = _
  val list = new util.ArrayList[FilterPredicate]()

  val tuple: () => Tuple = () =>
    Tuple
      .newBuilder()
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.BOOLEAN), true)
      .build()


  before {
    list.add(new FilterPredicate("field1",ComparisonType.NOT_NULL,"xx"))
    opDesc=new SpecializedFilterOpDesc()
    opDesc.predicates=list
    opExec = new SpecializedFilterOpExec(opDesc)
  }

  it should "open" in {

    opExec.open()

  }

  def layerID(): LayerIdentity = {
    LayerIdentity("1", "1", "1")
  }

  def linkID(): LinkIdentity = LinkIdentity(layerID(), layerID())


  it should "work with basic two input streams with no duplicates" in {
    (1 to 10).map(_ => {
      opExec.processTexeraTuple(Left(tuple()), linkID())
    })
    val outputTuples: List[Tuple] = opExec.processTexeraTuple(Right(InputExhausted()), linkID()).toList
    opExec.close()
  }
}
