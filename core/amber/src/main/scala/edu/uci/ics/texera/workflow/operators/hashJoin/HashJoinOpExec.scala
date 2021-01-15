package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.{InputExhausted, WorkflowLogger}
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinOpExec[K](val opDesc: HashJoinOpDesc[K]) extends OperatorExecutor {

  var hashJoinOpExecLogger = new WorkflowLogger(
    s"${opDesc.operatorIdentifier.getGlobalIdentity}-HashJoinOpExec"
  )

  var buildTableInputNum: Int = -1
  var isBuildTableFinished: Boolean = false
  var buildTableHashMap: mutable.HashMap[K, ArrayBuffer[Tuple]] = _

  var currentEntry: Iterator[Tuple] = _
  var currentTuple: Tuple = _

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int
  ): Iterator[Tuple] = {
    val buildTableTag: LayerTag =
      opDesc.hashJoinOpExecConfig.buildTableTag
    buildTableInputNum = opDesc.hashJoinOpExecConfig.getInputNum(
      OperatorIdentifier(buildTableTag.workflow, buildTableTag.operator)
    )
    tuple match {
      case Left(t) =>
        if (input == buildTableInputNum) {
          val key = t.getField(opDesc.buildAttribute).asInstanceOf[K]
          var storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          storedTuples += t
          buildTableHashMap.put(key, storedTuples)
          Iterator()
        } else {
          if (!isBuildTableFinished) {
            val err = WorkflowRuntimeError(
              "Probe table came before build table ended",
              "HashJoinOpExec",
              Map("stacktrace" -> Thread.currentThread().getStackTrace().mkString("\n"))
            )
            hashJoinOpExecLogger.logError(err)
            throw new WorkflowRuntimeException(err)
          } else {
            val key = t.getField(opDesc.probeAttribute).asInstanceOf[K]
            val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
            var tuplesToOutput: ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()
            storedTuples.foreach(buildTuple => {
              tuplesToOutput += Tuple
                .newBuilder()
                .add(buildTuple)
                .remove(opDesc.buildAttribute)
                .add(t)
                .build()
            })
            tuplesToOutput.iterator
          }
        }
      case Right(_) =>
        if (input == buildTableInputNum) {
          isBuildTableFinished = true
        }
        Iterator()

    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableInputNum = -1
    buildTableHashMap.clear()
  }
}
