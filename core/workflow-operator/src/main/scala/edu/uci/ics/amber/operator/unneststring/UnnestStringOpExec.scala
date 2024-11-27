package edu.uci.ics.amber.operator.unneststring

import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.flatmap.FlatMapOpExec

class UnnestStringOpExec(attributeName: String, delimiter: String) extends FlatMapOpExec {

  setFlatMapFunc(splitByDelimiter)

  private def splitByDelimiter(tuple: Tuple): Iterator[TupleLike] = {
    delimiter.r
      .split(tuple.getField(attributeName).toString)
      .filter(_.nonEmpty)
      .iterator
      .map(split => TupleLike(tuple.getFields ++ Seq(split)))
  }
}
