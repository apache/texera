package engine.architecture.worker

import java.util.concurrent.Executors

import engine.architecture.breakpoint.FaultedTuple
import engine.architecture.breakpoint.localbreakpoint.{ExceptionBreakpoint, LocalBreakpoint}
import engine.common.amberexception.BreakpointException
import engine.common.{
  AdvancedMessageSending,
  ElidableStatement,
  ThreadState,
  OperatorExecutor,
  SourceOperatorExecutor
}
import engine.common.ambermessage.WorkerMessage._
import engine.common.ambermessage.StateMessage._
import engine.common.ambermessage.ControlMessage._
import engine.common.ambertag.WorkerTag
import engine.common.tuple.Tuple
import engine.faulttolerance.recovery.RecoveryPacket
import akka.actor.{ActorLogging, Props, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.annotation.elidable
import scala.annotation.elidable.INFO
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.Breaks
import scala.concurrent.duration._

object Generator {
  def props(producer: SourceOperatorExecutor, tag: WorkerTag): Props = Props(new Generator(producer, tag))
}

class Generator(var dataProducer: SourceOperatorExecutor, val tag: WorkerTag)
    extends WorkerBase
    with ActorLogging
    with Stash {

  val dataGenerateExecutor: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)
  var isGeneratingFinished = false
  var outputIterator: Iterator[Tuple] = _

  var generatedCount = 0L
  @elidable(INFO) var generateTime = 0L
  @elidable(INFO) var generateStart = 0L

  override def onReset(value: Any, recoveryInformation: Seq[(Long, Long)]): Unit = {
    super.onReset(value, recoveryInformation)
    generatedCount = 0L
    dataProducer = value.asInstanceOf[SourceOperatorExecutor]
    dataProducer.open()
    resetBreakpoints()
    resetOutput()
    context.become(ready)
    self ! Start
  }

  override def onResuming(): Unit = {
    super.onResuming()
    Future {
      Generate()
    }(dataGenerateExecutor)
  }

  override def onCompleted(): Unit = {
    super.onCompleted()
    ElidableStatement.info {
      log.info(
        "completed its job. total: {} ms, generating: {} ms, generated {} tuples",
        (System.nanoTime() - startTime) / 1000000,
        generateTime / 1000000,
        generatedCount
      )
    }
  }

  override def onPaused(): Unit = {
    log.info(s"paused at $generatedCount , 0")
    context.parent ! ReportCurrentProcessingTuple(self.path, null)
    context.parent ! RecoveryPacket(tag, generatedCount, 0)
    context.parent ! ReportState(WorkerState.Paused)
  }

  private[this] def waitGenerating: Receive = {
    case ExecutionPaused =>
      onPaused()
      context.become(paused)
      unstashAll()
    case ReportFailure(e) =>
      throw e
    case ExecutionCompleted =>
      onCompleted()
      context.become(completed)
      unstashAll()
    case LocalBreakpointTriggered =>
      onBreakpointTriggered()
      context.become(paused)
      context.become(breakpointTriggered, discardOld = false)
      unstashAll()
    case QueryState =>
      sender ! ReportStatistics(
        WorkerStatistics(WorkerState.Pausing, generatedCount, generatedCount)
      )
    case msg => stash()
  }

  override def onResumeTuple(faultedTuple: FaultedTuple): Unit = {
    var i = 0
    while (i < output.length) {
      output(i).accept(faultedTuple.tuple)
      i += 1
    }
  }

  override def getInputRowCount(): Long = {
    this.generatedCount
  }

  override def getOutputRowCount(): Long = {
    this.generatedCount
  }

  override def onModifyTuple(faultedTuple: FaultedTuple): Unit = {
    userFixedTuple = faultedTuple.tuple
  }

  override def onPausing(): Unit = {
    super.onPausing()
    synchronized {
      if (!isGeneratingFinished) {
        ElidableStatement.finest { log.info("wait for generating thread") }
        context.become(waitGenerating)
        unstashAll()
      } else {
        onCompleted()
        context.become(completed)
        unstashAll()
      }
    }
  }

  override def onInitialization(recoveryInformation: Seq[(Long, Long)]): Unit = {
    super.onInitialization(recoveryInformation)
    dataProducer.open()
  }

  override def onInterrupted(operations: => Unit): Unit = {
    if (receivedRecoveryInformation.contains((generatedCount, 0))) {
      pausedFlag = true
      receivedRecoveryInformation.remove((generatedCount, 0))
    }
    super.onInterrupted(operations)
  }

  override def onStart(): Unit = {
    Future {
      Generate()
    }(dataGenerateExecutor)
    super.onStart()
    context.become(running)
    unstashAll()
  }

  private[this] def beforeGenerating(): Unit = {
    if (userFixedTuple != null) {
      try {
        transferTuple(userFixedTuple, generatedCount)
        userFixedTuple = null
      } catch {
        case e: BreakpointException =>
          self ! LocalBreakpointTriggered
          generateTime += System.nanoTime() - generateStart
          Breaks.break()
        case e: Exception =>
          self ! ReportFailure(e)
          generateTime += System.nanoTime() - generateStart
          Breaks.break()
      }
    }
  }

  private[this] def exitIfPaused(): Unit = {
    onInterrupted {
      self ! ExecutionPaused
      generateTime += System.nanoTime() - generateStart
    }
  }

  private[this] def Generate(): Unit = {
    Breaks.breakable {
      generateStart = System.nanoTime()
      beforeGenerating()
      this.outputIterator = dataProducer.produce()
      while (outputIterator.hasNext) {
        exitIfPaused()
        var nextTuple: Tuple = null
        try {
          nextTuple = outputIterator.next()
        } catch {
          case e: Exception =>
            self ! LocalBreakpointTriggered
            breakpoints(0).triggeredTuple = nextTuple
            breakpoints(0).asInstanceOf[ExceptionBreakpoint].error = e
            breakpoints(0).triggeredTupleId = generatedCount
            generateTime += System.nanoTime() - generateStart
            Breaks.break()
        }
        try {
          generatedCount += 1
          transferTuple(nextTuple, generatedCount)
        } catch {
          case e: BreakpointException =>
            self ! LocalBreakpointTriggered
            generateTime += System.nanoTime() - generateStart
            Breaks.break()
          case e: Exception =>
            self ! ReportFailure(e)
            generateTime += System.nanoTime() - generateStart
            Breaks.break()
        }
      }
      onCompleting()
      try {
        dataProducer.close()
      } catch {
        case e: Exception =>
          self ! ReportFailure(e)
          generateTime += System.nanoTime() - generateStart
          Breaks.break()
      }
      synchronized {
        isGeneratingFinished = true
        self ! ExecutionCompleted
      }
      generateTime += System.nanoTime() - generateStart
    }
  }
}
