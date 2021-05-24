package edu.uci.ics.texera.workflow.common.operators.mlmodel

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable.ListBuffer

abstract class MLModelOpExec() extends OperatorExecutor with Serializable {

  var allData: ListBuffer[Tuple] = ListBuffer()

  var currentEpoch: Int = 0
  var nextMiniBatchStartIdx: Int = 0
  var miniBatch: Array[Tuple] = _
  var MINI_BATCH_SIZE: Int = 1000
  var nextOperation: String = "predict"
  var hasMoreIterations: Boolean = true

  def getTotalEpochsCount: Int

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        allData += t
        Iterator()
      case Right(_) =>
        getIterativeTrainingIterator
    }
  }

  def getIterativeTrainingIterator: Iterator[Tuple] = {
    new Iterator[Tuple] {
      override def hasNext(): Boolean = {
        hasMoreIterations
      }

      override def next(): Tuple = {
        if (nextOperation.equalsIgnoreCase("predict")) {
          // set the miniBatch
          if (nextMiniBatchStartIdx + MINI_BATCH_SIZE <= allData.size) {
            miniBatch =
              allData.slice(nextMiniBatchStartIdx, nextMiniBatchStartIdx + MINI_BATCH_SIZE).toArray
            nextMiniBatchStartIdx = nextMiniBatchStartIdx + MINI_BATCH_SIZE
          } else if (nextMiniBatchStartIdx < allData.size) {
            // remaining data is less than MINI_BATCH_SIZE
            miniBatch = allData.slice(nextMiniBatchStartIdx, allData.size).toArray
            nextMiniBatchStartIdx = 0
          } else {
            // will reach if no data present in allData
            hasMoreIterations = false
            return null
          }

          predict(miniBatch)
          nextOperation = "calculateLossGradient"
        } else if (nextOperation.equalsIgnoreCase("calculateLossGradient")) {
          calculateLossGradient(miniBatch)
          nextOperation = "readjustWeight"
        } else if (nextOperation.equalsIgnoreCase("readjustWeight")) {
          readjustWeight()
          nextOperation = "predict"

          if (nextMiniBatchStartIdx == 0) {
            // current epoch is over
            currentEpoch += 1
          }
          if (currentEpoch == getTotalEpochsCount) {
            hasMoreIterations = false
          }
        }
        return null
      }
    }
  }

  def predict(miniBatch: Array[Tuple]): Unit
  def calculateLossGradient(miniBatch: Array[Tuple]): Unit
  def readjustWeight(): Unit

}
