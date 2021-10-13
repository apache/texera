package edu.uci.ics.texera.web.model.common

import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.common.tuple.amber.AmberTuple

object FaultedTupleFrontend {
  def apply(faultedTuple: FaultedTuple): FaultedTupleFrontend = {
    val tuple = faultedTuple.tuple
    val tupleList =
      if (tuple != null) {
        tuple.toArray().filter(v => v != null).map(v => v.toString).toList
      } else {
        List.empty
      }
    FaultedTupleFrontend(tupleList, faultedTuple.id, faultedTuple.isInput)
  }
}

case class FaultedTupleFrontend(tuple: List[String], id: Long, isInput: Boolean = false) {

  def toFaultedTuple: FaultedTuple = {
    val tupleList = this.tuple
    val amberTuple = new AmberTuple(tupleList.toArray)
    FaultedTuple(amberTuple, this.id, this.isInput)
  }

}
