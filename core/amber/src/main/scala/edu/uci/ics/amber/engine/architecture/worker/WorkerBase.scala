package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{Actor, ActorLogging, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkSenderActor.{
  NetworkAck,
  NetworkMessage,
  QueryActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  DataInputPort,
  DataOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.DummyInput
import edu.uci.ics.amber.engine.architecture.worker.neo._
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage._
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.annotation.elidable
import scala.annotation.elidable.INFO
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

abstract class WorkerBase(identifier: ActorVirtualIdentity) extends WorkflowActor(identifier) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  var operator: IOperatorExecutor

  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: DataInputPort = wire[DataInputPort]
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  val receivedRecoveryInformation: mutable.HashSet[(Long, Long)] =
    new mutable.HashSet[(Long, Long)]()

  var isCompleted = false
  @elidable(INFO) var startTime = 0L

  def onInitialization(recoveryInformation: Seq[(Long, Long)]): Unit = {
    receivedRecoveryInformation ++= recoveryInformation
  }

  def onSkipTuple(faultedTuple: FaultedTuple): Unit = {
    if (faultedTuple.isInput) {
      dataProcessor.setCurrentTuple(null)
    } else {
      // if it's output tuple, it will be ignored
    }
  }

  def onResumeTuple(faultedTuple: FaultedTuple): Unit = {}

  def onModifyTuple(faultedTuple: FaultedTuple): Unit = {}

  def onStart(): Unit = {
    logger.info(s"$identifier started!")
    startTime = System.nanoTime()
    context.parent ! ReportState(WorkerState.Running)
  }

  def onPausing(): Unit = {
    //messagingManager.pauseDataSending()
    pauseManager.pause()
    // if dp thread is blocking on waiting for input tuples:
    if (dataProcessor.isQueueEmpty) {
      // insert dummy batch to unblock dp thread
      dataProcessor.appendElement(DummyInput())
    }
    context.become(pausing)
  }

  def onPaused(): Unit = {
    context.parent ! ReportState(WorkerState.Paused)
  }

  def onResuming(): Unit = {
    //messagingManager.resumeDataSending()
  }

  def onResumed(): Unit = {
    context.parent ! ReportState(WorkerState.Running)
  }

  def onCompleted(): Unit = {
    isCompleted = true
    context.parent ! ReportState(WorkerState.Completed)
  }

  def onBreakpointTriggered(): Unit = {
    dataProcessor.breakpoints.foreach { brk =>
      if (brk.isTriggered)
        dataProcessor.unhandledFaultedTuples(brk.triggeredTupleId) =
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
    receivedFaultedTupleIds.clear()
  }

  def getResultTuples(): mutable.MutableList[ITuple] = {
    mutable.MutableList()
  }

  final def allowStashOrReleaseOutput: Receive = {
    case StashOutput =>
      sender ! Ack
    case ReleaseOutput =>
      sender ! Ack
  }

  final def allowModifyBreakpoints: Receive = {
    case AssignBreakpoint(bp) =>
      dataProcessor.registerBreakpoint(bp)
      sender ! Ack
    case RemoveBreakpoint(id) =>
      sender ! Ack
      dataProcessor.removeBreakpoint(id)

  }

  final def disallowModifyBreakpoints: Receive = {
    case AssignBreakpoint(bp) =>
      sender ! Ack
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          s"Assignation of breakpoint ${bp.id} is not allowed at this time",
          "WorkerBase:disallowModifyBreakpoints",
          Map()
        )
      )
    case RemoveBreakpoint(id) =>
      sender ! Ack
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          s"Removal of breakpoint $id is not allowed at this time",
          "WorkerBase:RemoveBreakpoint",
          Map()
        )
      )
  }

  final def allowReset: Receive = {
    case Reset(core, rec) =>
      onReset(core, rec)
  }

  final def allowQueryBreakpoint: Receive = {
    case QueryBreakpoint(id) =>
      val toReport = dataProcessor.breakpoints.find(_.id == id)
      if (toReport.isDefined) {
        toReport.get.isReported = true
        context.parent ! ReportedQueriedBreakpoint(toReport.get)
      } else {
        throw new WorkflowRuntimeException(
          WorkflowRuntimeError(
            s"breakpoint $id not found when query",
            "WorkerBase:allowQueryBreakpoint",
            Map()
          )
        )
      }
  }

  final def disallowQueryBreakpoint: Receive = {
    case QueryBreakpoint(id) =>
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          s"query breakpoint $id is not allowed at this time",
          "WorkerBase:disallowQueryBreakpoint",
          Map()
        )
      )
  }

  final def allowQueryTriggeredBreakpoints: Receive = {
    case QueryTriggeredBreakpoints =>
      val toReport = dataProcessor.breakpoints.filter(_.isTriggered)
      if (toReport.nonEmpty) {
        toReport.foreach(_.isReported = true)
        sender ! ReportedTriggeredBreakpoints(toReport)
      } else {
        throw new WorkflowRuntimeException(
          WorkflowRuntimeError(
            "no triggered local breakpoints but worker in triggered breakpoint state",
            "WorkerBase:allowQueryTriggeredBreakpoints",
            Map()
          )
        )
      }
  }

  final def disallowQueryTriggeredBreakpoints: Receive = {
    case QueryTriggeredBreakpoints =>
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          s"query triggered breakpoints is not allowed at this time",
          "WorkerBase:disallowQueryTriggeredBreakpoints",
          Map()
        )
      )
  }

  final def allowUpdateOutputLinking: Receive = {
    case UpdateOutputLinking(policy, tag, receivers) =>
      sender ! Ack
      batchProducer.addPolicy(policy, tag, receivers)
  }

  final def disallowUpdateOutputLinking: Receive = {
    case UpdateOutputLinking(policy, tag, receivers) =>
      sender ! Ack
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          s"update output link information of $tag is not allowed at this time",
          "WorkerBase:disallowUpdateOutputLinking",
          Map()
        )
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
      logger.info(s"stashing in WorkerBase" + msg)
      stash()
  }

  final def discardOthers: Receive = {
    case msg => logger.info(s"discarding: $msg")
  }

  override def receive: Receive = {
    routeActorRefRelatedMessages orElse
      processNewControlMessages orElse [Any, Unit] {
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
  }

  def ready: Receive =
    allowStashOrReleaseOutput orElse
      routeActorRefRelatedMessages orElse
      processNewControlMessages orElse
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
      routeActorRefRelatedMessages orElse
      processNewControlMessages orElse
      allowUpdateOutputLinking orElse
      allowModifyBreakpoints orElse
      disallowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case QueryBreakpoint(id) =>
        val toReport = dataProcessor.breakpoints.find(_.id == id)
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
      processNewControlMessages orElse
      routeActorRefRelatedMessages orElse
      allowUpdateOutputLinking orElse
      allowModifyBreakpoints orElse
      disallowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case Resume =>
        dataProcessor.unhandledFaultedTuples.values.foreach(onResumeTuple)
        dataProcessor.unhandledFaultedTuples.clear()
        onResuming()
        onResumed()
        context.become(running)
        unstashAll()
      case SkipTuple(f) =>
        sender ! Ack
        if (!receivedFaultedTupleIds.contains(f.id)) {
          receivedFaultedTupleIds.add(f.id)
          dataProcessor.unhandledFaultedTuples.remove(f.id)
          onSkipTuple(f)
        }
      case ModifyTuple(f) =>
        sender ! Ack
        if (!receivedFaultedTupleIds.contains(f.id)) {
          receivedFaultedTupleIds.add(f.id)
          dataProcessor.unhandledFaultedTuples.remove(f.id)
          onModifyTuple(f)
        }
      case ResumeTuple(f) =>
        sender ! Ack
        if (!receivedFaultedTupleIds.contains(f.id)) {
          receivedFaultedTupleIds.add(f.id)
          dataProcessor.unhandledFaultedTuples.remove(f.id)
          onResumeTuple(f)
        }
      case Pause      => context.parent ! ReportState(WorkerState.Paused)
      case QueryState => sender ! ReportState(WorkerState.Paused)
      case QueryStatistics =>
        sender ! ReportStatistics(
          WorkerStatistics(WorkerState.Paused, getInputRowCount(), getOutputRowCount())
        )
      case QueryBreakpoint(id) =>
        val toReport = dataProcessor.breakpoints.find(_.id == id)
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
        throw new WorkflowRuntimeException(
          WorkflowRuntimeError(
            "breakpoint triggered after pause",
            "WorkerBase:paused:LocalBreakpointTriggered",
            Map()
          )
        )
    } orElse discardOthers

  def running: Receive =
    processNewControlMessages orElse [Any, Unit] {
      case ReportFailure(e) =>
        logger.info(s"received failure message")
        throw e
      case Pause =>
        logger.info(s"$identifier received Pause message")
        onPausing()
      case LocalBreakpointTriggered =>
        logger.info("receive breakpoint triggered")
        onBreakpointTriggered()
        context.become(paused)
        context.become(breakpointTriggered, discardOld = false)
        unstashAll()
      case ExecutionCompleted =>
        logger.info("received complete")
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
      processNewControlMessages orElse
      routeActorRefRelatedMessages orElse
      allowUpdateOutputLinking orElse
      allowQueryBreakpoint orElse
      allowQueryTriggeredBreakpoints orElse [Any, Unit] {
      case AssignBreakpoint(bp) =>
        sender ! Ack
        dataProcessor.registerBreakpoint(bp)
        if (!dataProcessor.breakpoints.exists(_.isDirty)) {
          onPaused() //back to paused
          context.unbecome()
          unstashAll()
        }
      case RemoveBreakpoint(id) =>
        sender ! Ack
        dataProcessor.removeBreakpoint(id)
        if (!dataProcessor.breakpoints.exists(_.isDirty)) {
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
      case Resume | Pause           => context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
      case LocalBreakpointTriggered => //discard this
    } orElse stashOthers

  def completed: Receive =
    allowReset orElse allowStashOrReleaseOutput orElse
      routeActorRefRelatedMessages orElse
      disallowUpdateOutputLinking orElse
      processNewControlMessages orElse
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

  def pausing: Receive = {
    processNewControlMessages orElse {
      case msg =>
        //stash all other messages
        stash()
    }
  }

  def newControlMessageHandler: Receive = {
    case ExecutionCompleted() =>
      logger.info("received complete")
      onCompleted()
      context.become(completed)
      unstashAll()
    case ExecutionPaused() =>
      logger.info(s"received Execution Pause message")
      onPaused()
      context.become(paused)
      unstashAll()
  }

  def processNewControlMessages: Receive = {
    case msg @ NetworkMessage(id, cmd: WorkflowControlMessage) =>
      //println(s"received $msg")
      sender ! NetworkAck(id)
      controlInputPort.handleControlMessage(cmd)
      newControlMessageHandler(cmd.payload)
  }

}
