package edu.uci.ics.amber.operator.limit

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class LimitOpExec(descString: String) extends OperatorExecutor {
  private val desc: LimitOpDesc = objectMapper.readValue(descString, classOf[LimitOpDesc])
  var count = 0

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    if (count < desc.limit) {
      count += 1
      Iterator(tuple)
    } else {
      Iterator()
    }

  }

}
