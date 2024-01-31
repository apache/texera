package edu.uci.ics.texera.workflow.operators.limit

import edu.uci.ics.amber.engine.architecture.worker.DataProcessor.ForLoopIterationCompleted
import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class LimitOpExec(val limit: Int) extends OperatorExecutor {
  var count = 0
  var i = 0;


  override def processTuple(tuple: Either[ITuple, InputExhausted], input: Int, pauseManager: PauseManager, asyncRPCClient: AsyncRPCClient): Iterator[(ITuple, Option[PortIdentity])] = {
    tuple match {
      case Left(t) =>
        t match{
          case ForLoopIterationCompleted() => Iterator((t, None))
          case _ => // process
        }
        Iterator.empty //skip
      case Right(_) => Iterator((ForLoopIterationCompleted(), None))
    }
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (count < limit) {
          count += 1
          Iterator(t)
        } else {
          Iterator()
        }
      case Right(_) => Iterator(ForLoopIterationCompleted())
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
