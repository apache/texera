package edu.uci.ics.amber.engine.architecture.principal

import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.amberexception.AmberException
import edu.uci.ics.amber.engine.common.ambermessage.PrincipalMessage.{AssignBreakpoint, _}
import edu.uci.ics.amber.engine.common.ambermessage.StateMessage._
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{
  Ack,
  AckWithInformation,
  CollectSinkResults,
  KillAndRecover,
  LocalBreakpointTriggered,
  ModifyLogic,
  ModifyTuple,
  Pause,
  QueryState,
  QueryStatistics,
  ReleaseOutput,
  RequireAck,
  Resume,
  ResumeTuple,
  SkipTuple,
  Start,
  StashOutput
}
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.ReportGlobalBreakpointTriggered
import edu.uci.ics.amber.engine.common.ambermessage.{PrincipalMessage, WorkerMessage}
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  AckedWorkerInitialization,
  CheckRecovery,
  DataMessage,
  EndSending,
  ExecutionCompleted,
  ExecutionPaused,
  QueryBreakpoint,
  QueryTriggeredBreakpoints,
  RemoveBreakpoint,
  ReportFailure,
  ReportWorkerPartialCompleted,
  ReportedQueriedBreakpoint,
  ReportedTriggeredBreakpoints,
  Reset,
  UpdateOutputLinking
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier, WorkerTag}
import edu.uci.ics.amber.engine.common.{
  AdvancedMessageSending,
  AmberUtils,
  Constants,
  TableMetadata,
  ITupleSinkOperatorExecutor
}
import edu.uci.ics.amber.engine.faulttolerance.recovery.RecoveryPacket
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{
  Actor,
  ActorLogging,
  ActorPath,
  ActorRef,
  Address,
  Cancellable,
  PoisonPill,
  Props,
  Stash
}
import akka.event.LoggingAdapter
import akka.util.Timeout
import akka.pattern.after
import akka.pattern.ask
import com.google.common.base.Stopwatch

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Principal {
  def props(metadata: OpExecConfig): Props = Props(new Principal(metadata))
}

class Principal(val metadata: OpExecConfig) extends Actor with ActorLogging with Stash {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

  val tau: FiniteDuration = Constants.defaultTau
  var workerLayers: Array[ActorLayer] = _
  var workerEdges: Array[LinkStrategy] = _
  var layerDependencies: mutable.HashMap[String, mutable.HashSet[String]] = _
  var workerStateMap: mutable.AnyRefMap[ActorRef, WorkerState.Value] = _
  var workerStatisticsMap: mutable.AnyRefMap[ActorRef, WorkerStatistics] = _
  var workerSinkResultMap = new mutable.AnyRefMap[ActorRef, List[ITuple]]
  var layerMetadata: Array[TableMetadata] = _
  var isUserPaused = false
  var globalBreakpoints = new mutable.AnyRefMap[String, GlobalBreakpoint]
  var periodicallyAskHandle: Cancellable = _
  var workersTriggeredBreakpoint: Iterable[ActorRef] = _
  var layerCompletedCounter: mutable.HashMap[LayerTag, Int] = _
  val timer = Stopwatch.createUnstarted();
  val stage1Timer = Stopwatch.createUnstarted();
  val stage2Timer = Stopwatch.createUnstarted();
  var receivedRecoveryInformation = new mutable.HashMap[AmberTag, (Long, Long)]()
  val receivedTuples = new mutable.ArrayBuffer[(ITuple, ActorPath)]()

  def allWorkerStates: Iterable[WorkerState.Value] = workerStateMap.values
  def allWorkers: Iterable[ActorRef] = workerStateMap.keys
  def unCompletedWorkerStates: Iterable[WorkerState.Value] =
    workerStateMap.filter(x => x._2 != WorkerState.Completed).values
  def unCompletedWorkers: Iterable[ActorRef] =
    workerStateMap.filter(x => x._2 != WorkerState.Completed).keys
  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]

  private def setWorkerStatistics(worker: ActorRef, workerStatistics: WorkerStatistics): Unit = {
    workerStatisticsMap.update(worker, workerStatistics)
  }

  private def aggregateWorkerInputRowCount(): Long = {
    workerStatisticsMap.values.map(s => s.inputRowCount).sum
  }

  private def aggregateWorkerOutputRowCount(): Long = {
    workerStatisticsMap.values.map(s => s.outputRowCount).sum
  }

  private def setWorkerState(worker: ActorRef, state: WorkerState.Value): Boolean = {
    assert(workerStateMap.contains(worker))
    //only set when state changes.
    if (workerStateMap(worker) != state) {
      if (WorkerState.ValidTransitions(workerStateMap(worker)).contains(state)) {
        workerStateMap(worker) = state
      } else if (WorkerState.SkippedTransitions(workerStateMap(worker)).contains(state)) {
        log.info(
          "Skipped worker state transition for worker{} from {} to {}",
          worker,
          workerStateMap(worker),
          state
        )
        workerStateMap(worker) = state
      } else {
        log.warning(
          "Invalid worker state transition for worker{} from {} to {}",
          worker,
          workerStateMap(worker),
          state
        )
      }
      true
    } else false
  }

  final def whenAllUncompletedWorkersBecome(state: WorkerState.Value): Boolean =
    unCompletedWorkerStates.forall(_ == state)
  final def whenAllWorkersCompleted: Boolean = allWorkerStates.forall(_ == WorkerState.Completed)
  final def safeRemoveAskHandle(): Unit = {
    if (periodicallyAskHandle != null) {
      periodicallyAskHandle.cancel()
      periodicallyAskHandle = null
    }
  }

  final def resetAll(): Unit = {
    workerLayers = null
    workerEdges = null
    layerDependencies = null
    workerStateMap = null
    layerMetadata = null
    isUserPaused = false
    safeRemoveAskHandle()
    periodicallyAskHandle = null
    workersTriggeredBreakpoint = null
    layerCompletedCounter = null
    globalBreakpoints.foreach(_._2.reset())
    timer.reset()
    stage1Timer.reset()
    stage2Timer.reset()
    context.become(receive)
  }

  final def ready: Receive = {
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = (seq1, seq2)
    case Start =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, Start, 10, 0)
      )
    case WorkerMessage.ReportState(state) =>
      setWorkerState(sender, state)
      state match {
        case WorkerState.Running =>
          context.parent ! ReportState(PrincipalState.Running)
          context.become(running)
          timer.start()
          stage1Timer.start()
          unstashAll()
        case WorkerState.Paused =>
          if (whenAllUncompletedWorkersBecome(WorkerState.Paused)) {
            safeRemoveAskHandle()
            context.parent ! ReportState(PrincipalState.Paused)
            context.become(paused)
            unstashAll()
          }
        case _ => //throw new AmberException("Invalid worker state received!")
      }
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Ready,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case StashOutput =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, StashOutput, 10, 0)
      )
    case ReleaseOutput =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, ReleaseOutput, 10, 0)
      )
    case GetInputLayer  => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case QueryState     => sender ! ReportState(PrincipalState.Ready)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
    case Resume => context.parent ! ReportState(PrincipalState.Ready)
    case AssignBreakpoint(breakpoint) =>
      globalBreakpoints(breakpoint.id) = breakpoint
      log.info("assign breakpoint: " + breakpoint.id)
      metadata.assignBreakpoint(workerLayers, workerStateMap, breakpoint)
      sender ! Ack
    case Pause =>
      allWorkers.foreach(worker => worker ! Pause)
      safeRemoveAskHandle()
      periodicallyAskHandle =
        context.system.scheduler.schedule(0.milliseconds, 30.seconds, self, EnforceStateCheck)
      context.become(pausing)
      unstashAll()
    case msg => stash()
  }

  final def running: Receive = {
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = (seq1, seq2)
    case WorkerMessage.ReportState(state) =>
      log.info("running: " + sender + " to " + state)
      if (setWorkerState(sender, state)) {
        state match {
          case WorkerState.LocalBreakpointTriggered =>
            if (whenAllUncompletedWorkersBecome(WorkerState.LocalBreakpointTriggered)) {
              //only one worker and it triggered breakpoint
              safeRemoveAskHandle()
              periodicallyAskHandle = context.system.scheduler.schedule(
                0.milliseconds,
                30.seconds,
                self,
                EnforceStateCheck
              )
              workersTriggeredBreakpoint = allWorkers
              context.parent ! ReportState(PrincipalState.CollectingBreakpoints)
              context.become(collectingBreakpoints)
            } else {
              //no tau involved since we know a very small tau works best
              if (!stage2Timer.isRunning) {
                stage2Timer.start()
              }
              if (stage1Timer.isRunning) {
                stage1Timer.stop()
              }
              context.system.scheduler
                .scheduleOnce(tau, () => unCompletedWorkers.foreach(worker => worker ! Pause))
              safeRemoveAskHandle()
              periodicallyAskHandle =
                context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
              context.become(pausing)
              unstashAll()
            }
          case WorkerState.Paused =>
            if (whenAllWorkersCompleted) {
              safeRemoveAskHandle()
              context.parent ! ReportState(PrincipalState.Completed)
              context.become(completed)
              unstashAll()
            } else if (whenAllUncompletedWorkersBecome(WorkerState.Paused)) {
              safeRemoveAskHandle()
              context.parent ! ReportState(PrincipalState.Paused)
              context.become(paused)
              unstashAll()
            } else if (
              unCompletedWorkerStates
                .forall(x => x == WorkerState.Paused || x == WorkerState.LocalBreakpointTriggered)
            ) {
              workersTriggeredBreakpoint =
                workerStateMap.filter(_._2 == WorkerState.LocalBreakpointTriggered).keys
              safeRemoveAskHandle()
              periodicallyAskHandle = context.system.scheduler.schedule(
                1.milliseconds,
                30.seconds,
                self,
                EnforceStateCheck
              )
              context.parent ! ReportState(PrincipalState.CollectingBreakpoints)
              context.become(collectingBreakpoints)
              unstashAll()
            }
          case WorkerState.Completed =>
            if (whenAllWorkersCompleted) {
              if (timer.isRunning) {
                timer.stop()
              }
              log.info(metadata.tag.toString + " completed! Time Elapsed: " + timer.toString())
              context.parent ! ReportState(PrincipalState.Completed)
              context.become(completed)
              unstashAll()
            }
          case _ => //skip others for now
        }
      }
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Running,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case Pause =>
      //single point pause: pause itself
      if (sender != self) {
        isUserPaused = true
      }
      allWorkers.foreach(worker => worker ! Pause)
      safeRemoveAskHandle()
      periodicallyAskHandle =
        context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
      context.become(pausing)
      unstashAll()
    case ReportWorkerPartialCompleted(worker, layer) =>
      sender ! Ack
      AdvancedMessageSending.nonBlockingAskWithRetry(
        context.parent,
        ReportPrincipalPartialCompleted(worker, layer),
        10,
        0
      )
      if (layerCompletedCounter.contains(layer)) {
        layerCompletedCounter(layer) -= 1
        if (layerCompletedCounter(layer) == 0) {
          layerCompletedCounter -= layer
          AdvancedMessageSending.nonBlockingAskWithRetry(
            context.parent,
            ReportPrincipalPartialCompleted(metadata.tag, layer),
            10,
            0
          )
        }
      }
    case StashOutput =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, StashOutput, 10, 0)
      )
    case ReleaseOutput =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, ReleaseOutput, 10, 0)
      )
    case GetInputLayer  => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case Resume         => context.parent ! ReportState(PrincipalState.Running)
    case QueryState     => sender ! ReportState(PrincipalState.Running)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  final lazy val allowedStatesOnPausing: Set[WorkerState.Value] =
    Set(WorkerState.Completed, WorkerState.Paused, WorkerState.LocalBreakpointTriggered)

  final def pausing: Receive = {
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = (seq1, seq2)
    case EnforceStateCheck =>
      for ((k, v) <- workerStateMap) {
        if (!allowedStatesOnPausing.contains(v)) {
          k ! QueryState
        }
      }
    case reportCurrentTuple: WorkerMessage.ReportCurrentProcessingTuple =>
      receivedTuples.append((reportCurrentTuple.tuple, reportCurrentTuple.workerID))
    case WorkerMessage.ReportState(state) =>
      log.info("pausing: " + sender + " to " + state)
      if (setWorkerState(sender, state)) {
        if (whenAllWorkersCompleted) {
          safeRemoveAskHandle()
          context.parent ! ReportState(PrincipalState.Completed)
          context.become(completed)
          unstashAll()
        } else if (whenAllUncompletedWorkersBecome(WorkerState.Paused)) {
          safeRemoveAskHandle()
          context.parent ! ReportCurrentProcessingTuple(
            this.metadata.tag.operator,
            receivedTuples.toArray
          )
          receivedTuples.clear()
          context.parent ! ReportState(PrincipalState.Paused)
          context.become(paused)
          unstashAll()
        } else if (
          unCompletedWorkerStates
            .forall(x => x == WorkerState.Paused || x == WorkerState.LocalBreakpointTriggered)
        ) {
          workersTriggeredBreakpoint =
            workerStateMap.filter(_._2 == WorkerState.LocalBreakpointTriggered).keys
          safeRemoveAskHandle()
          periodicallyAskHandle =
            context.system.scheduler.schedule(1.milliseconds, 30.seconds, self, EnforceStateCheck)
          context.parent ! ReportState(PrincipalState.CollectingBreakpoints)
          context.become(collectingBreakpoints)
          unstashAll()
        }
      }
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Pausing,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case QueryState => sender ! ReportState(PrincipalState.Pausing)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
    case Pause =>
      if (sender != self) {
        isUserPaused = true
      }
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  final def collectingBreakpoints: Receive = {
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = (seq1, seq2)
    case EnforceStateCheck =>
      workersTriggeredBreakpoint.foreach(x => x ! QueryTriggeredBreakpoints) //query all
    case WorkerMessage.ReportState(state) =>
      //log.info("collecting: "+ sender +" to "+ state)
      if (setWorkerState(sender, state)) {
        if (unCompletedWorkerStates.forall(_ == WorkerState.Paused)) {
          //all breakpoint resolved, it's safe to report to controller and then Pause(on triggered, or user paused) else Resume
          val map = new mutable.HashMap[(ActorRef, FaultedTuple), ArrayBuffer[String]]
          for (i <- globalBreakpoints.values.filter(_.isTriggered)) {
            isUserPaused = true //upgrade pause
            i.report(map)
          }
          safeRemoveAskHandle()
          context.become(paused)
          unstashAll()
          if (!isUserPaused) {
            log.info("no global breakpoint triggered, continue")
            self ! Resume
          } else {
            context.parent ! ReportGlobalBreakpointTriggered(map, this.metadata.tag.operator)
            context.parent ! ReportState(PrincipalState.Paused)
            log.info(
              "user paused or global breakpoint triggered, pause. Stage1 cost = " + stage1Timer
                .toString() + " Stage2 cost =" + stage2Timer.toString()
            )
          }
          if (stage2Timer.isRunning) {
            stage2Timer.stop()
          }
          if (!stage1Timer.isRunning) {
            stage1Timer.start()
          }
        }
      }
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.CollectingBreakpoints,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case ReportedTriggeredBreakpoints(bps) =>
      bps.foreach(x => {
        val bp = globalBreakpoints(x.id)
        bp.accept(sender, x)
        if (bp.needCollecting) {
          //is not fully collected
          bp.collect()
        } else if (bp.isRepartitionRequired) {
          //fully collected, but need repartition (e.g. count not reach target number)
          //OR need Reset
          metadata.assignBreakpoint(workerLayers, workerStateMap, bp)
        } else if (bp.isCompleted) {
          //fully collected and reach the target
          bp.remove()
        }
      })
    case ReportedQueriedBreakpoint(bp) =>
      val gbp = globalBreakpoints(bp.id)
      if (gbp.accept(sender, bp) && !gbp.needCollecting) {
        if (gbp.isRepartitionRequired) {
          //fully collected, but need repartition (count not reach target number)
          metadata.assignBreakpoint(workerLayers, workerStateMap, gbp)
        } else if (gbp.isCompleted) {
          //fully collected and reach the target
          gbp.remove()
        }
      }
    case GetInputLayer  => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case Pause =>
      if (sender != self) {
        isUserPaused = true
      }
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  final lazy val allowedStatesOnResuming: Set[WorkerState.Value] =
    Set(WorkerState.Running, WorkerState.Ready, WorkerState.Completed)

  final def resuming: Receive = {
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = (seq1, seq2)
    case EnforceStateCheck =>
      for ((k, v) <- workerStateMap) {
        if (!allowedStatesOnResuming.contains(v)) {
          k ! QueryState
        }
      }
    case WorkerMessage.ReportState(state) =>
      //log.info("resuming: "+ sender +" to "+ state)
      if (!allowedStatesOnResuming.contains(state)) {
        sender ! Resume
      } else if (setWorkerState(sender, state)) {
        if (whenAllWorkersCompleted) {
          safeRemoveAskHandle()
          context.parent ! ReportState(PrincipalState.Completed)
          context.become(completed)
          unstashAll()
        } else if (allWorkerStates.forall(_ != WorkerState.Paused)) {
          safeRemoveAskHandle()
          if (allWorkerStates.exists(_ != WorkerState.Ready)) {
            context.parent ! ReportState(PrincipalState.Running)
            context.become(running)
          } else {
            context.parent ! ReportState(PrincipalState.Ready)
            context.become(ready)
          }
          unstashAll()
        }
      }
    case GetInputLayer  => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Resuming,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case QueryState => sender ! ReportState(PrincipalState.Resuming)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  final def paused: Receive = {
    case KillAndRecover =>
      workerLayers.foreach { x =>
        x.layer(0) ! Reset(x.getFirstMetadata, Seq(receivedRecoveryInformation(x.tagForFirst)))
        workerStateMap(x.layer(0)) = WorkerState.Ready
      }
      sender ! Ack
      context.become(pausing)
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = receivedRecoveryInformation(amberTag)
    case Resume =>
      isUserPaused = false //reset
      assert(unCompletedWorkerStates.nonEmpty)
      unCompletedWorkers.foreach(worker => worker ! Resume)
      safeRemoveAskHandle()
      periodicallyAskHandle =
        context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
      context.become(resuming)
      unstashAll()
    case AssignBreakpoint(breakpoint) =>
      sender ! Ack
      globalBreakpoints(breakpoint.id) = breakpoint
      metadata.assignBreakpoint(workerLayers, workerStateMap, breakpoint)
    case GetInputLayer  => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case Pause          => context.parent ! ReportState(PrincipalState.Paused)
    case QueryState     => sender ! ReportState(PrincipalState.Paused)
    case ModifyLogic(newMetadata) =>
      sender ! Ack
      log.info("modify logic received by principal, sending to worker")
      this.allWorkers.foreach(worker => worker ! ModifyLogic(newMetadata))
//      allWorkers.foreach(worker => AdvancedMessageSending.blockingAskWithRetry(worker, ModifyLogic(newMetadata), 3))
      log.info("modify logic received  by principal, sent to worker")
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Paused,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  final def completed: Receive = {
    case KillAndRecover =>
      workerLayers.foreach { x =>
        if (receivedRecoveryInformation.contains(x.tagForFirst)) {
          x.layer(0) ! Reset(x.getFirstMetadata, Seq(receivedRecoveryInformation(x.tagForFirst)))
        } else {
          x.layer(0) ! Reset(x.getFirstMetadata, Seq())
        }
        workerStateMap(x.layer(0)) = WorkerState.Ready
      }
      sender ! Ack
      context.become(pausing)
    case RecoveryPacket(amberTag, seq1, seq2) =>
      receivedRecoveryInformation(amberTag) = (seq1, seq2)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
    case StashOutput =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, StashOutput, 10, 0)
      )
    case ReleaseOutput =>
      sender ! Ack
      allWorkers.foreach(worker =>
        AdvancedMessageSending.nonBlockingAskWithRetry(worker, ReleaseOutput, 10, 0)
      )
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
      context.parent ! PrincipalMessage.ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Completed,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case CollectSinkResults =>
      allWorkers.foreach(worker => worker ! CollectSinkResults)
    case WorkerMessage.ReportOutputResult(sinkResult) =>
      workerSinkResultMap(sender) = sinkResult
      if (workerSinkResultMap.size == allWorkers.size) {
        val collectedResults = mutable.MutableList[ITuple]()
        this.workerSinkResultMap.values.foreach(v => collectedResults ++= v)
        context.parent ! PrincipalMessage.ReportOutputResult(collectedResults.toList)
      }
    case GetInputLayer  => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case msg            =>
      //log.info("received {} from {} after complete",msg,sender)
      if (sender == context.parent) {
        sender ! ReportState(PrincipalState.Completed)
      }
  }

  final override def receive: Receive = {
    case AckedPrincipalInitialization(prev: Array[(OpExecConfig, ActorLayer)]) =>
      workerLayers = metadata.topology.layers
      workerEdges = metadata.topology.links
      val all = availableNodes
      if (workerEdges.isEmpty) {
        workerLayers.foreach(x => x.build(prev, all))
      } else {
        val inLinks: Map[ActorLayer, Set[ActorLayer]] =
          workerEdges.groupBy(x => x.to).map(x => (x._1, x._2.map(_.from).toSet))
        var currentLayer: Iterable[ActorLayer] =
          workerEdges.filter(x => workerEdges.forall(_.to != x.from)).map(_.from)
        currentLayer.foreach(x => x.build(prev, all))
        currentLayer = inLinks.filter(x => x._2.forall(_.isBuilt)).keys
        while (currentLayer.nonEmpty) {
          currentLayer.foreach(x => x.build(inLinks(x).map(y => (null, y)).toArray, all))
          currentLayer = inLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
        }
      }
      layerCompletedCounter =
        mutable.HashMap(prev.map(x => x._2.tag -> workerLayers.head.layer.length).toSeq: _*)
      workerStateMap = mutable.AnyRefMap(
        workerLayers.flatMap(x => x.layer).map((_, WorkerState.Uninitialized)).toMap.toSeq: _*
      )
      workerStatisticsMap = mutable.AnyRefMap(
        workerLayers
          .flatMap(x => x.layer)
          .map((_, WorkerStatistics(WorkerState.Uninitialized, 0, 0)))
          .toMap
          .toSeq: _*
      )
      workerLayers.foreach { x =>
        var i = 0
        x.layer.foreach { worker =>
          val workerTag = WorkerTag(x.tag, i)
          worker ! AckedWorkerInitialization()
          i += 1
        }
      }
      safeRemoveAskHandle()
      periodicallyAskHandle =
        context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
      context.become(initializing)
      unstashAll()
      sender ! AckWithInformation(metadata)
    case QueryState => sender ! ReportState(PrincipalState.Uninitialized)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
      sender() ! ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Uninitialized,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  final def initializing: Receive = {
    case EnforceStateCheck =>
      for ((k, v) <- workerStateMap) {
        if (v != WorkerState.Ready) {
          k ! QueryState
        }
      }
    case WorkerMessage.ReportState(state) =>
      if (state != WorkerState.Ready) {
        sender ! AckedWorkerInitialization()
      } else if (setWorkerState(sender, state)) {
        if (whenAllUncompletedWorkersBecome(WorkerState.Ready)) {
          safeRemoveAskHandle()
          workerEdges.foreach(x => x.link())
          globalBreakpoints.values.foreach(
            metadata.assignBreakpoint(workerLayers, workerStateMap, _)
          )
          allWorkers.foreach(_ ! CheckRecovery)
          context.parent ! ReportState(PrincipalState.Ready)
          context.become(ready)
          unstashAll()
        }
      }
    case WorkerMessage.ReportStatistics(statistics) =>
      setWorkerStatistics(sender, statistics)
    case QueryState => sender ! ReportState(PrincipalState.Initializing)
    case QueryStatistics =>
      this.allWorkers.foreach(worker => worker ! QueryStatistics)
      sender() ! ReportStatistics(
        PrincipalStatistics(
          PrincipalState.Initializing,
          aggregateWorkerInputRowCount(),
          aggregateWorkerOutputRowCount()
        )
      )
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

}
