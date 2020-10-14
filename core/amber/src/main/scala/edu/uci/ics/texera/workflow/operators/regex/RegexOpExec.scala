package edu.uci.ics.texera.workflow.operators.regex

import java.util.regex.Pattern

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class RegexOpExec(val opDesc: RegexOpDesc) extends FilterOpExec {
  val pattern = Pattern.compile(opDesc.regex)
  this.setFilterFunc(this.matchRegex)

  def matchRegex(tuple: Tuple): Boolean = {
    val tupleValue = tuple.getField(opDesc.attribute).toString.trim
    pattern.matcher(tupleValue).find
  }

}
