package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorLogging, Props, Stash}
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.worker.neo.PauseManager
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  DummyInput,
  EndMarker,
  EndOfAllMarker
}
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage._
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{
  ActorVirtualIdentity,
  WorkerActorVirtualIdentity
}
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, WorkerTag}
import edu.uci.ics.amber.engine.common.{
  ElidableStatement,
  IOperatorExecutor,
  ISourceOperatorExecutor
}
import edu.uci.ics.amber.engine.faulttolerance.recovery.RecoveryPacket

import scala.annotation.elidable
import scala.annotation.elidable.INFO

object Generator {
  def props(producer: ISourceOperatorExecutor, tag: WorkerTag): Props =
    Props(new Generator(producer, tag))
}

class Generator(var operator: IOperatorExecutor, val tag: WorkerTag)
    extends WorkerBase(WorkerActorVirtualIdentity(tag.getGlobalIdentity))
    with ActorLogging
    with Stash {

  @elidable(INFO) var generateTime = 0L
  @elidable(INFO) var generateStart = 0L

  override def onReset(value: Any, recoveryInformation: Seq[(Long, Long)]): Unit = {
    super.onReset(value, recoveryInformation)
    operator = value.asInstanceOf[ISourceOperatorExecutor]
    operator.open()
    dataProcessor.resetBreakpoints()
    batchProducer.resetPolicies()
    context.become(ready)
    self ! Start
  }

  override def onResuming(): Unit = {
    super.onResuming()
    pauseManager.resume()
  }

  override def onCompleted(): Unit = {
    super.onCompleted()
    ElidableStatement.info {
      val (_, outputCount) = dataProcessor.collectStatistics()
      log.info(
        "completed its job. total: {} ms, generating: {} ms, generated {} tuples",
        (System.nanoTime() - startTime) / 1000000,
        generateTime / 1000000,
        outputCount
      )
    }
  }

  override def onPaused(): Unit = {
    val (inputCount, outputCount) = dataProcessor.collectStatistics()
    log.info(s"${tag.getGlobalIdentity} paused at $outputCount , 0")
    context.parent ! ReportCurrentProcessingTuple(self.path, dataProcessor.getCurrentInputTuple)
    context.parent ! RecoveryPacket(tag, outputCount, 0)
    context.parent ! ReportState(WorkerState.Paused)
  }

  override def onResumeTuple(faultedTuple: FaultedTuple): Unit = {
    batchProducer.passTupleToDownstream(faultedTuple.tuple)
  }

  override def getInputRowCount(): Long = {
    0 // source operator should not have input rows
  }

  override def getOutputRowCount(): Long = {
    val (_, outputCount) = dataProcessor.collectStatistics()
    outputCount
  }

  override def onModifyTuple(faultedTuple: FaultedTuple): Unit = {
    batchProducer.passTupleToDownstream(faultedTuple.tuple)
  }

  override def onInitialization(recoveryInformation: Seq[(Long, Long)]): Unit = {
    super.onInitialization(recoveryInformation)
    operator.open()
  }

  override def onStart(): Unit = {
    super.onStart()
    dataProcessor.appendElement(EndMarker())
    dataProcessor.appendElement(EndOfAllMarker())
    context.become(running)
    unstashAll()
  }

}
