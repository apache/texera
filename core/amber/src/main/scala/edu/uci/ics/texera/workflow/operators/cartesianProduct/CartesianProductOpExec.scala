package edu.uci.ics.texera.workflow.operators.cartesianProduct

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.JoinUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Executes a Cartesian Product operation between tuples from two input streams.
  */
class CartesianProductOpExec extends OperatorExecutor {

  private var leftTuples: ArrayBuffer[Tuple] = _

  /**
    * Processes incoming tuples from either the left or right input stream.
    * Tuples from the left stream are collected until the stream is exhausted.
    * Then, for each tuple from the right stream, it generates a Cartesian product
    * with every tuple collected from the left stream.
    *
    * @param tuple Either a Tuple from one of the streams or an InputExhausted signal.
    * @param port The port number indicating which stream the tuple is from (0 for left, 1 for right).
    * @return An Iterator of TupleLike objects representing the Cartesian product.
    */
  override def processTuple(tuple: Either[Tuple, InputExhausted], port: Int): Iterator[TupleLike] =
    tuple match {
      case Left(tuple) =>
        if (port == 0) {
          leftTuples += tuple
          Iterator.empty
        } else {
          leftTuples.map(leftTuple => JoinUtils.joinTuples(leftTuple, tuple)).iterator
        }
      case Right(_) =>
        Iterator.empty
    }

  override def open(): Unit = {
    leftTuples = ArrayBuffer[Tuple]()
  }

  override def close(): Unit = leftTuples.clear()
}
