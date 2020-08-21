package web.model.common

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Common.AmberTuple.AmberTuple

object FaultedTupleFrontend {
  def apply(faultedTuple: FaultedTuple): FaultedTupleFrontend = {
    val tuple = faultedTuple.tuple
    val tupleList = faultedTuple.tuple.toArray().filter(v => v != null).map(v => v.toString).toList
    FaultedTupleFrontend(tupleList, faultedTuple.id, faultedTuple.isInput)
  }
}

case class FaultedTupleFrontend(tuple: List[String], id: Long, isInput: Boolean = false) {

  def toFaultedTuple(): FaultedTuple = {
    val tupleList = this.tuple
    val amberTuple = new AmberTuple(tupleList.toArray)
    new FaultedTuple(amberTuple, this.id, this.isInput)
  }

}
