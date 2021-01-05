package edu.uci.ics.texera.workflow.operators.regex

import java.util.regex.Pattern

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class RegexOpExec(val opDesc: RegexOpDesc) extends FilterOpExec {
  val pattern: Pattern =
    if (opDesc.caseInsensitive) Pattern.compile(opDesc.regex, Pattern.CASE_INSENSITIVE)
    else Pattern.compile(opDesc.regex)
  this.setFilterFunc(this.matchRegex)

  def matchRegex(tuple: Tuple): Boolean = {
    val tupleValue = Option[Any](tuple.getField(opDesc.attribute)).map(x => x.toString)
    if (tupleValue.isEmpty)
      false
    else
      pattern.matcher(tupleValue.get).find
  }

}
