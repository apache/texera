package texera.operators.regex

import java.util.regex.Pattern

import texera.common.operators.filter.TexeraFilterOpExec
import texera.common.tuple.TexeraTuple

class RegexOpExec(val opDesc: RegexOpDesc) extends TexeraFilterOpExec {
  val pattern = Pattern.compile(opDesc.regex)
  this.setFilterFunc(this.matchRegex)

  def matchRegex(tuple: TexeraTuple): Boolean = {
    val tupleValue = tuple.getField(opDesc.attribute).toString.trim
    pattern.matcher(tupleValue).find
  }

}
