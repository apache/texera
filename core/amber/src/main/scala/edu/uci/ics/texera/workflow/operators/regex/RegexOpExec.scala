package edu.uci.ics.texera.workflow.operators.regex

import edu.uci.ics.amber.engine.common.model.tuple.Tuple
import java.util.regex.Pattern

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec

class RegexOpExec(regex: String, caseInsensitive: Boolean, attributeName: String)
    extends FilterOpExec {
  lazy val pattern: Pattern =
    Pattern.compile(regex, if (caseInsensitive) Pattern.CASE_INSENSITIVE else 0)
  this.setFilterFunc(this.matchRegex)
  private def matchRegex(tuple: Tuple): Boolean =
    Option[Any](tuple.getField(attributeName).toString)
      .map(_.toString)
      .exists(value => pattern.matcher(value).find)
}
