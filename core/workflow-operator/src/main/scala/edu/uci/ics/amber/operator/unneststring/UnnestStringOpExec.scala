package edu.uci.ics.amber.operator.unneststring

import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.flatmap.FlatMapOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class UnnestStringOpExec(descString: String) extends FlatMapOpExec {
  private val desc: UnnestStringOpDesc =
    objectMapper.readValue(descString, classOf[UnnestStringOpDesc])
  setFlatMapFunc(splitByDelimiter)

  private def splitByDelimiter(tuple: Tuple): Iterator[TupleLike] = {
    desc.delimiter.r
      .split(tuple.getField(desc.attribute).toString)
      .filter(_.nonEmpty)
      .iterator
      .map(split => TupleLike(tuple.getFields ++ Seq(split)))
  }
}
