package edu.uci.ics.amber.operator.filter

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class SpecializedFilterOpExec(descString: String) extends FilterOpExec {
  private val desc: SpecializedFilterOpDesc =
    objectMapper.readValue(descString, classOf[SpecializedFilterOpDesc])
  setFilterFunc((tuple: Tuple) => desc.predicates.exists(_.evaluate(tuple)))
}
