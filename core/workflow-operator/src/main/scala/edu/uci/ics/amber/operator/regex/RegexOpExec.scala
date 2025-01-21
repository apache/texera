package edu.uci.ics.amber.operator.regex

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.operator.filter.FilterOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.util.regex.Pattern

class RegexOpExec(descString: String) extends FilterOpExec {
  private val desc: RegexOpDesc = objectMapper.readValue(descString, classOf[RegexOpDesc])
  lazy val pattern: Pattern =
    Pattern.compile(desc.regex, if (desc.caseInsensitive) Pattern.CASE_INSENSITIVE else 0)
  this.setFilterFunc(this.matchRegex)

  private def matchRegex(tuple: Tuple): Boolean =
    Option[Any](tuple.getField(desc.attribute).toString)
      .map(_.toString)
      .exists(value => pattern.matcher(value).find)
}
