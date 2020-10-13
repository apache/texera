package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.common.amberexception.AmberException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage._
import edu.uci.ics.amber.engine.common.tuple.Tuple
import edu.uci.ics.amber.engine.common.ElidableStatement
import akka.actor.{Actor, ActorLogging, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.annotation.elidable
import scala.annotation.elidable.INFO
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks
import scala.concurrent.duration._

abstract class WorkerBase extends Actor with ActorLogging with Stash with DataTransferSupport {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  val receivedRecoveryInformation: mutable.HashSet[(Long, Long)] =
    new mutable.HashSet[(Long, Long)]()

  var pausedFlag = false
  var userFixedTuple: Tuple = _
  @elidable(INFO) var startTime = 0L

  def onInitialization(recoveryInformation: Seq[(Long, Long)]): Unit = {
    receivedRecoveryInformation ++= recoveryInformation
  }

  def onSkipTuple(faultedTuple: FaultedTuple): Unit = {
    if (faultedTuple.isInput) {
      skippedInputTuples.add(faultedTuple.tuple)
    } else {
      skippedOutputTuples.add(faultedTuple.tuple)
    }
  }

  def onResumeTuple(faultedTuple: FaultedTuple): Unit = {}

  def onModifyTuple(faultedTuple: FaultedTuple): Unit = {}

  def onStart(): Unit = {
    log.info("started!")
    startTime = System.nanoTime()
    context.parent ! ReportState(WorkerState.Running)
  }

  def onPausing(): Unit = {
    pausedFlag = true
    pauseDataTransfer()
  }

  def onPaused(): Unit = {
    context.parent ! ReportState(WorkerState.Paused)
  }

  def onResuming(): Unit = {
    resumeDataTransfer()
    pausedFlag = false
  }

  def onResumed(): Unit = {
    context.parent ! ReportState(WorkerState.Running)
  }

  def onCompleting(): Unit = {
    endDataTransfer()
  }

  def onCompleted(): Unit = {
    context.parent ! ReportState(WorkerState.Completed)
  }

  def onInterrupted(operations: => Unit): Unit = {
    if (pausedFlag) {
      //pauseDataTransfer()
      operations
      Breaks.break()
    }
  }

  def onBreakpointTriggered(): Unit = {
    breakpoints.foreach { brk =>
      if (brk.isTriggered)
        unhandledFaultedTuples(brk.triggeredTupleId) =
          new FaultedTuple(brk.triggeredTuple, brk.triggeredTupleId, brk.isInput)
    }
    context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
  }

  def getInputRowCount(): Long

  def getOutputRowCount(): Long

  def onReset(value: Any, recoveryInformation: Seq[(Long, Long)]): Unit = {
//    Thread.sleep(1000)
    receivedRecoveryInformation.clear()
    receivedRecoveryInformation ++= recoveryInformation
    userFixedTuple = null
    receivedFaultedTupleIds.clear()
    pausedFlag = false
  }

  def getResultTuples(): mutable.MutableList[Tuple] = {
    mutable.MutableList()
  }

  final def allowStashOrReleaseOutput: Receive = {
    case StashOutput =>
      sender ! Ack
      pauseDataTransfer()
    case ReleaseOutput =>
      sender ! Ack
      resumeDataTransfer()
  }

  final def allowModifyBreakpoints: Receive = {
    case AssignBreakpoint(bp) =>
      log.info("Assign breakpoint: " + bp.id)
      registerBreakpoint(bp)
      sender ! Ack
    case RemoveBreakpoint(id) =>
      log.info("Remove breakpoint: " + id)
      sender ! Ack
      removeBreakpoint(id)

  }

  final def disallowModifyBreakpoints: Receive = {
    case AssignBreakpoint(bp) =>
      sender ! Ack
      throw new AmberException(s"Assignation of breakpoint ${bp.id} is not allowed at this time")
    case RemoveBreakpoint(id) =>
      sender ! Ack
      throw new AmberException(s"Removal of breakpoint $id is not allowed at this time")
  }

  final def allowReset: Receive = {
    case Reset(core, rec) =>
      onReset(core, rec)
  }

  final def allowQueryBreakpoint: Receive = {
    case QueryBreakpoint(id) =>
      val toReport = breakpoints.find(_.id == id)
      if (toReport.isDefined) {
        toReport.get.isReported = true
        context.parent ! ReportedQueriedBreakpoint(toReport.get)
      } else {
        throw new AmberException(s"breakpoint $id not found when query")
      }
  }

  final def disallowQueryBreakpoint: Receive = {
    case QueryBreakpoint(id) =>
      throw new AmberException(s"query breakpoint $id is not allowed at this time")
  }

  final def allowQueryTriggeredBreakpoints: Receive = {
    case QueryTriggeredBreakpoints =>
      val toReport = breakpoints.filter(_.isTriggered)
      if (toReport.nonEmpty) {
        toReport.foreach(_.isReported = true)
        sender ! ReportedTriggeredBreakpoints(toReport)
      } else {
        throw new AmberException(
          "no triggered local breakpoints but worker in triggered breakpoint state"
        )
      }
  }

  final def disallowQueryTriggeredBreakpoints: Receive = {
    case QueryTriggeredBreakpoints =>
      throw new AmberException(s"query triggered breakpoints is not allowed at this time")
  }

  final def allowUpdateOutputLinking: Receive = {
    case UpdateOutputLinking(policy, tag, receivers) =>
      sender ! Ack
      updateOutput(policy, tag, receivers)
  }

  final def disallowUpdateOutputLinking: Receive = {
    case UpdateOutputLinking(policy, tag, receivers) =>
      sender ! Ack
      throw new AmberException(
        s"update output link information of $tag is not allowed at this time"
      )
  }

  final def allowCheckRecovery: Receive = {
    case CheckRecovery =>
      if (receivedRecoveryInformation.contains((0, 0))) {
        receivedRecoveryInformation.remove((0, 0))
        self ! Pause
      }
  }

  final def disallowCheckRecovery: Receive = {
    case CheckRecovery =>
    //Skip
  }

  final def stashOthers: Receive = {
    case msg =>
      log.info("stashing: " + msg)
      stash()
  }

  final def discardOthers: Receive = {
    case msg => log.info(s"discarding: $msg")
  }

  override def receive: Receive = {
    case AckedWorkerInitialization(recoveryInformation) =>
      onInitialization(recoveryInformation)
      context.parent ! ReportState(WorkerState.Ready)
      context.become(ready)
      unstashAll()
    case QueryState =>
      sender ! ReportState(WorkerState.Uninitialized)
    case QueryStatistics =>
      sender ! ReportStatistics(
        WorkerStatistics(WorkerState.Uninitialized, getInputRowCount(), getOutputRowCount())
      )
    case _ => stash()
  }

  def ready: Receive =
    allowStashOrReleaseOutput orElse
      allowUpdateOutputLinking orElse //update linking
      allowModifyBreakpoints orElse //modify break points
      disallowQueryBreakpoint orElse //query specific breakpoint
      allowCheckRecovery orElse
      disallowQueryTriggeredBreakpoints orElse [Any, Unit] { //query triggered breakpoint
      case Start =>
        sender ! Ack
        onStart()
      case Pause =>
        onPaused()
        context.become(pausedBeforeStart)
        unstashAll()
      case Resume     => context.parent ! ReportState(WorkerState.Ready)
      case QueryState => sender ! ReportState(WorkerState.Ready)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(WorkerState.Ready, getInputRowCount(), getOutputRowCount())
        )
    } orElse discardOthers

  def pausedBeforeStart: Receive =
    allowReset orElse allowStashOrReleaseOutput orElse
      allowUpdateOutputLinking orElse
      allowModifyBreakpoints orElse
      disallowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case QueryBreakpoint(id) =>
        val toReport = breakpoints.find(_.id == id)
        if (toReport.isDefined) {
          toReport.get.isReported = true
          context.parent ! ReportedQueriedBreakpoint(toReport.get)
          context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
          context.become(breakpointTriggered, discardOld = false)
          unstashAll()
        }
      case Resume =>
        context.parent ! ReportState(WorkerState.Ready)
        context.become(ready)
        unstashAll()
      case Pause      => context.parent ! ReportState(WorkerState.Paused)
      case QueryState => sender ! ReportState(WorkerState.Paused)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(WorkerState.Paused, getInputRowCount(), getOutputRowCount())
        )
    } orElse discardOthers

  def paused: Receive =
    allowReset orElse
      allowStashOrReleaseOutput orElse
      allowUpdateOutputLinking orElse
      allowModifyBreakpoints orElse
      disallowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case Resume =>
        unhandledFaultedTuples.values.foreach(onResumeTuple)
        unhandledFaultedTuples.clear()
        onResuming()
        onResumed()
        context.become(running)
        unstashAll()
      case SkipTuple(f) =>
        sender ! Ack
        if (!receivedFaultedTupleIds.contains(f.id)) {
          receivedFaultedTupleIds.add(f.id)
          unhandledFaultedTuples.remove(f.id)
          onSkipTuple(f)
        }
      case ModifyTuple(f) =>
        sender ! Ack
        if (!receivedFaultedTupleIds.contains(f.id)) {
          receivedFaultedTupleIds.add(f.id)
          unhandledFaultedTuples.remove(f.id)
          onModifyTuple(f)
        }
      case ResumeTuple(f) =>
        sender ! Ack
        if (!receivedFaultedTupleIds.contains(f.id)) {
          receivedFaultedTupleIds.add(f.id)
          unhandledFaultedTuples.remove(f.id)
          onResumeTuple(f)
        }
      case Pause      => context.parent ! ReportState(WorkerState.Paused)
      case QueryState => sender ! ReportState(WorkerState.Paused)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(WorkerState.Paused, getInputRowCount(), getOutputRowCount())
        )
      case QueryBreakpoint(id) =>
        val toReport = breakpoints.find(_.id == id)
        if (toReport.isDefined) {
          toReport.get.isReported = true
          context.parent ! ReportedQueriedBreakpoint(toReport.get)
          context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
          context.become(breakpointTriggered, discardOld = false)
          unstashAll()
        }
      case CollectSinkResults =>
        sender ! WorkerMessage.ReportOutputResult(this.getResultTuples().toList)
      case LocalBreakpointTriggered =>
        throw new AmberException("breakpoint triggered after pause")
    } orElse discardOthers

  def running: Receive =
    allowReset orElse allowStashOrReleaseOutput orElse
      disallowUpdateOutputLinking orElse
      disallowModifyBreakpoints orElse
      disallowQueryBreakpoint orElse
      disallowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case ReportFailure(e) =>
        throw e
      case ExecutionPaused =>
        onPaused()
        context.become(paused)
        unstashAll()
      case Pause =>
        log.info("received Pause message")
        onPausing()
      case LocalBreakpointTriggered =>
        log.info("receive breakpoint triggered")
        onBreakpointTriggered()
        context.become(paused)
        context.become(breakpointTriggered, discardOld = false)
        unstashAll()
      case ExecutionCompleted =>
        log.info("received complete")
        onCompleted()
        context.become(completed)
        unstashAll()
      case Resume     => context.parent ! ReportState(WorkerState.Running)
      case QueryState => sender ! ReportState(WorkerState.Running)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(WorkerState.Running, getInputRowCount(), getOutputRowCount())
        )
      case CollectSinkResults =>
        sender ! WorkerMessage.ReportOutputResult(this.getResultTuples().toList)
    } orElse discardOthers

  def breakpointTriggered: Receive =
    allowStashOrReleaseOutput orElse
      allowUpdateOutputLinking orElse
      allowQueryBreakpoint orElse
      allowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case AssignBreakpoint(bp) =>
        log.info("assign breakpoint: " + bp)
        sender ! Ack
        registerBreakpoint(bp)
        if (!breakpoints.exists(_.isDirty)) {
          onPaused() //back to paused
          context.unbecome()
          unstashAll()
        }
      case RemoveBreakpoint(id) =>
        log.info("remove breakpoint: " + id)
        sender ! Ack
        removeBreakpoint(id)
        if (!breakpoints.exists(_.isDirty)) {
          onPaused() //back to paused
          context.unbecome()
          unstashAll()
        }
      case QueryState => sender ! ReportState(WorkerState.LocalBreakpointTriggered)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(
            WorkerState.LocalBreakpointTriggered,
            getInputRowCount(),
            getOutputRowCount()
          )
        )
      case DataMessage(_, _) | EndSending(_) => stash()
      case Resume | Pause                    => context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
      case LocalBreakpointTriggered          => //discard this
    } orElse stashOthers

  def completed: Receive =
    allowReset orElse allowStashOrReleaseOutput orElse
      disallowUpdateOutputLinking orElse
      allowModifyBreakpoints orElse
      allowQueryBreakpoint orElse [Any, Unit] {
      case QueryState => sender ! ReportState(WorkerState.Completed)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(WorkerState.Completed, getInputRowCount(), getOutputRowCount())
        )
      case QueryTriggeredBreakpoints => //skip this
      case ExecutionCompleted        => //skip this as well
      case CollectSinkResults =>
        sender ! WorkerMessage.ReportOutputResult(this.getResultTuples().toList)
      case msg =>
        if (sender == context.parent) {
          sender ! ReportState(WorkerState.Completed)
        }
    }

}
