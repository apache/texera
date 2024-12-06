package edu.uci.ics.amber.core.executor

import edu.uci.ics.amber.core.marker.State
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.workflow.PortIdentity

trait OperatorExecutor {

  def open(): Unit = {}

  def produceStateOnStart(port: Int): Option[State] = None

  def processState(state: State, port: Int): Option[State] = {
    if (state.isPassToAllDownstream) {
      Some(state)
    } else {
      None
    }
  }

  def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    processTuple(tuple, port).map(t => (t, None))
  }

  def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike]

  def produceStateOnFinish(port: Int): Option[State] = None

  def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    onFinish(port).map(t => (t, None))
  }

  def onFinish(port: Int): Iterator[TupleLike] = Iterator.empty

  def close(): Unit = {}

}
