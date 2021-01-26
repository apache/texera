package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  BreakpointTriggered,
  ErrorOccurred,
  ModifyLogicCompleted,
  SkipTupleResponse,
  WorkflowCompleted,
  WorkflowPaused,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.OneOnEach
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.faulttolerance.materializer.{
  HashBasedMaterializer,
  OutputMaterializer
}
import edu.uci.ics.amber.engine.architecture.linksemantics.{
  FullRoundRobin,
  HashBasedShuffle,
  LinkStrategy,
  LocalPartialToOne,
  OperatorLink
}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage._
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControllerMessage,
  PrincipalMessage,
  WorkerMessage
}
import edu.uci.ics.amber.engine.common.ambermessage.PrincipalMessage.{
  AckedPrincipalInitialization,
  AssignBreakpoint,
  GetOutputLayer,
  ReportCurrentProcessingTuple,
  ReportOutputResult,
  ReportPrincipalPartialCompleted,
  ReportState
}
import edu.uci.ics.amber.engine.common.ambermessage.StateMessage.EnforceStateCheck
import edu.uci.ics.amber.engine.common.ambertag.{
  AmberTag,
  LayerTag,
  LinkTag,
  OperatorIdentifier,
  WorkerTag,
  WorkflowTag
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{
  AdvancedMessageSending,
  AmberUtils,
  Constants,
  ISourceOperatorExecutor,
  WorkflowLogger
}
import edu.uci.ics.amber.engine.faulttolerance.scanner.HDFSFolderScanSourceOperatorExecutor
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{
  Actor,
  ActorLogging,
  ActorPath,
  ActorRef,
  ActorSelection,
  Address,
  Cancellable,
  Deploy,
  PoisonPill,
  Props,
  Stash
}
import akka.dispatch.Futures
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import com.github.nscala_time.time.Imports.DateTime
import com.google.common.base.Stopwatch
import play.api.libs.json.{JsArray, JsValue, Json, __}
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
import com.softwaremill.macwire.wire
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.worker.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  AckedWorkerInitialization,
  CheckRecovery,
  QueryTriggeredBreakpoints,
  ReportWorkerPartialCompleted,
  ReportedQueriedBreakpoint,
  ReportedTriggeredBreakpoints,
  Reset
}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.RegisterActorRef
import edu.uci.ics.amber.engine.architecture.principal.{PrincipalState, PrincipalStatistics}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object Controller {

  def props(
      tag: WorkflowTag,
      workflow: Workflow,
      withCheckpoint: Boolean,
      eventListener: ControllerEventListener,
      statusUpdateInterval: Long
  ): Props =
    Props(
      new Controller(
        tag,
        workflow,
        withCheckpoint,
        eventListener,
        Option.apply(statusUpdateInterval)
      )
    )
}

class Controller(
    val tag: WorkflowTag,
    val workflow: Workflow,
    val withCheckpoint: Boolean,
    val eventListener: ControllerEventListener = ControllerEventListener(),
    val statisticsUpdateIntervalMs: Option[Long]
) extends WorkflowActor(VirtualIdentity.Controller, null) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  // register controller itself
  networkCommunicationActor ! RegisterActorRef(VirtualIdentity.Controller, self)

  lazy val rpcHandlerInitializer = wire[AsyncRPCHandlerInitializer]

  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
  }
  val controllerLogger = WorkflowLogger(s"Controller-${tag.getGlobalIdentity}-Logger")
  controllerLogger.setErrorLogAction(errorLogAction)

  val edges = new mutable.AnyRefMap[LinkTag, OperatorLink]
  val frontier = new mutable.HashSet[OperatorIdentifier]
  var prevFrontier: mutable.HashSet[OperatorIdentifier] = _
  val stashedFrontier = new mutable.HashSet[OperatorIdentifier]
  val stashedNodes = new mutable.HashSet[OperatorIdentifier]()
  val linksToIgnore = new mutable.HashSet[(OperatorIdentifier, OperatorIdentifier)]
  var periodicallyAskHandle: Cancellable = _
  var statusUpdateAskHandle: Cancellable = _
  var startDependencies =
    new mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[
      LayerTag
    ]]]
  val timer = Stopwatch.createUnstarted();
  val pauseTimer = Stopwatch.createUnstarted();
  var recoveryMode = false

  def allUnCompletedOperatorStates: Iterable[PrincipalState.Value] =
    operatorStateMap.filter(x => x._2 != PrincipalState.Completed).values
  val tau: FiniteDuration = Constants.defaultTau
  var operatorToPrincipalSinkResultMap = new mutable.HashMap[String, List[ITuple]]
  var operatorToWorkerLayers = new mutable.HashMap[OperatorIdentifier, Array[WorkerLayer]]()
  var workerToOperator = new mutable.HashMap[ActorRef, OperatorIdentifier]()
  var operatorToWorkerEdges = new mutable.HashMap[OperatorIdentifier, Array[LinkStrategy]]()
  var operatorStateMap = new mutable.AnyRefMap[OperatorIdentifier, PrincipalState.Value]
  var operatorInCurrentStage = new mutable.HashSet[OperatorIdentifier]()
  var operatorToWorkerStateMap =
    new mutable.HashMap[OperatorIdentifier, mutable.AnyRefMap[ActorRef, WorkerState.Value]]()
  var operatorToWorkerStatisticsMap =
    new mutable.HashMap[OperatorIdentifier, mutable.AnyRefMap[ActorRef, WorkerStatistics]]()
  var operatorToWorkerSinkResultMap =
    new mutable.HashMap[OperatorIdentifier, mutable.AnyRefMap[ActorRef, List[
      ITuple
    ]]]() // initialize the values
  var operatorToIsUserPaused =
    new mutable.HashMap[OperatorIdentifier, Boolean]() // initialize to false
  var operatorToGlobalBreakpoints =
    new mutable.HashMap[OperatorIdentifier, mutable.AnyRefMap[String, GlobalBreakpoint]]()
  var operatorToPeriodicallyAskHandle = new mutable.HashMap[OperatorIdentifier, Cancellable]()
  var operatorToLayerCompletedCounter =
    new mutable.HashMap[OperatorIdentifier, mutable.HashMap[LayerTag, Int]]()
  val operatorToTimer =
    new mutable.HashMap[
      OperatorIdentifier,
      Stopwatch
    ]() // initialize with Stopwatch.createUnstarted();
  val operatorToStage1Timer =
    new mutable.HashMap[
      OperatorIdentifier,
      Stopwatch
    ]() // initialize with Stopwatch.createUnstarted();
  val operatorToStage2Timer =
    new mutable.HashMap[
      OperatorIdentifier,
      Stopwatch
    ]() // initialize with Stopwatch.createUnstarted();
  var operatorToReceivedRecoveryInformation =
    new mutable.HashMap[OperatorIdentifier, mutable.HashMap[AmberTag, (Long, Long)]]()
  val operatorToReceivedTuples =
    new mutable.HashMap[OperatorIdentifier, mutable.ArrayBuffer[(ITuple, ActorPath)]]()

  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]

  final def safeRemoveAskHandle(): Unit = {
    if (periodicallyAskHandle != null) {
      periodicallyAskHandle.cancel()
      periodicallyAskHandle = null
    }
  }

  private def aggregateWorkerInputRowCount(opIdentifier: OperatorIdentifier): Long = {
    operatorToWorkerStatisticsMap(opIdentifier)
      .filter(e => operatorToWorkerLayers(opIdentifier).head.layer.contains(e._1))
      .map(e => e._2.inputRowCount)
      .sum
  }

  // the output count is the sum of the output counts of the last-layer actors
  private def aggregateWorkerOutputRowCount(opIdentifier: OperatorIdentifier): Long = {
    operatorToWorkerStatisticsMap(opIdentifier)
      .filter(e => operatorToWorkerLayers(opIdentifier).last.layer.contains(e._1))
      .map(e => e._2.outputRowCount)
      .sum
  }

  def triggerStatusUpdateEvent(): Unit = {
    if (
      this.eventListener.workflowStatusUpdateListener != null
      && this.operatorToWorkerStatisticsMap.nonEmpty
    ) {
      var workflowStatus = new mutable.HashMap[String, PrincipalStatistics]()
      operatorStateMap.keys.foreach(opIdentifier => {

        workflowStatus(opIdentifier.operator) = PrincipalStatistics(
          operatorStateMap(opIdentifier),
          aggregateWorkerInputRowCount(opIdentifier),
          aggregateWorkerOutputRowCount(opIdentifier)
        )
      })
      this.eventListener.workflowStatusUpdateListener
        .apply(WorkflowStatusUpdate(workflowStatus.toMap))
    }
  }

  private def initializeOperatorDataStructures(op: OperatorIdentifier): Unit = {
    operatorToWorkerSinkResultMap(op) = new mutable.AnyRefMap[ActorRef, List[ITuple]]
    operatorToIsUserPaused(op) = false
    operatorToGlobalBreakpoints(op) = new mutable.AnyRefMap[String, GlobalBreakpoint]
    operatorToTimer(op) = Stopwatch.createUnstarted()
    operatorToStage1Timer(op) = Stopwatch.createUnstarted()
    operatorToStage2Timer(op) = Stopwatch.createUnstarted()
    operatorToReceivedRecoveryInformation(op) = new mutable.HashMap[AmberTag, (Long, Long)]()
    operatorToReceivedTuples(op) = new mutable.ArrayBuffer[(ITuple, ActorPath)]()
    operatorStateMap(op) = PrincipalState.Uninitialized
    operatorInCurrentStage.add(op)
  }

  private def safeRemoveAskOperatorHandle(op: OperatorIdentifier): Unit = {
    if (
      operatorToPeriodicallyAskHandle.contains(op) && operatorToPeriodicallyAskHandle(op) != null
    ) {
      operatorToPeriodicallyAskHandle(op).cancel()
      operatorToPeriodicallyAskHandle(op) = null
    }
  }

  private def setWorkerState(worker: ActorRef, state: WorkerState.Value): Boolean = {
    val operatorIdentifier = workerToOperator(worker)
    assert(operatorToWorkerStateMap(operatorIdentifier).contains(worker))
    //only set when state changes.
    if (operatorToWorkerStateMap(operatorIdentifier)(worker) != state) {
      if (
        WorkerState
          .ValidTransitions(operatorToWorkerStateMap(operatorIdentifier)(worker))
          .contains(state)
      ) {
        operatorToWorkerStateMap(operatorIdentifier)(worker) = state
      } else if (
        WorkerState
          .SkippedTransitions(operatorToWorkerStateMap(operatorIdentifier)(worker))
          .contains(state)
      ) {
        operatorToWorkerStateMap(operatorIdentifier)(worker) = state
      }
      true
    } else false
  }

  final def whenAllUncompletedWorkersBecome(
      operatorIdentifier: OperatorIdentifier,
      state: WorkerState.Value
  ): Boolean =
    operatorToWorkerStateMap(operatorIdentifier)
      .filter(x => x._2 != WorkerState.Completed)
      .values
      .forall(_ == state)

  private def initializeOperators(
      startOp: OperatorIdentifier,
      prev: Array[(OpExecConfig, WorkerLayer)]
  ): Unit = {
    var metadata =
      workflow.operators(startOp) // This metadata gets updated at the end of this function
    operatorToWorkerLayers(startOp) = metadata.topology.layers
    operatorToWorkerEdges(startOp) = metadata.topology.links
    val all = availableNodes
    if (operatorToWorkerEdges(startOp).isEmpty) {
      operatorToWorkerLayers(startOp).foreach(x =>
        x.build(prev, all, networkCommunicationActor.ref)
      )
    } else {
      val inLinks: Map[WorkerLayer, Set[WorkerLayer]] =
        operatorToWorkerEdges(startOp).groupBy(x => x.to).map(x => (x._1, x._2.map(_.from).toSet))
      var currentLayer: Iterable[WorkerLayer] =
        operatorToWorkerEdges(startOp)
          .filter(x => operatorToWorkerEdges(startOp).forall(_.to != x.from))
          .map(_.from)
      currentLayer.foreach(x => x.build(prev, all, networkCommunicationActor.ref))
      currentLayer = inLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (currentLayer.nonEmpty) {
        currentLayer.foreach(x =>
          x.build(inLinks(x).map(y => (null, y)).toArray, all, networkCommunicationActor.ref)
        )
        currentLayer = inLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
      }
    }
    operatorToLayerCompletedCounter(startOp) = mutable.HashMap(
      prev.map(x => x._2.tag -> operatorToWorkerLayers(startOp).head.layer.length).toSeq: _*
    )
    operatorToWorkerStateMap(startOp) = mutable.AnyRefMap(
      operatorToWorkerLayers(startOp)
        .flatMap(x => x.layer)
        .map((_, WorkerState.Uninitialized))
        .toMap
        .toSeq: _*
    )
    operatorToWorkerStatisticsMap(startOp) = mutable.AnyRefMap(
      operatorToWorkerLayers(startOp)
        .flatMap(x => x.layer)
        .map((_, WorkerStatistics(WorkerState.Uninitialized, 0, 0)))
        .toMap
        .toSeq: _*
    )
    operatorToWorkerLayers(startOp).foreach { x =>
      var i = 0
      x.identifiers.indices.foreach(i =>
        networkCommunicationActor ! RegisterActorRef(x.identifiers(i), x.layer(i))
      )
      x.layer.foreach { worker =>
        val workerTag = WorkerTag(x.tag, i)
        workerToOperator(worker) = startOp
        worker ! AckedWorkerInitialization()
        i += 1
      }
    }
    safeRemoveAskOperatorHandle(startOp)
    operatorToPeriodicallyAskHandle(startOp) =
      context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck(startOp))
  }

  private def initializingNextFrontier(): Unit = {
    if (
      operatorStateMap.size == workflow.operators.size && operatorStateMap.values.forall(
        _ != PrincipalState.Uninitialized
      )
    ) {
      frontier.clear()
      if (stashedFrontier.nonEmpty) {
        controllerLogger.logInfo("partially initialized!")
        frontier ++= stashedFrontier
        stashedFrontier.clear()
      } else {
        controllerLogger.logInfo("fully initialized!")
      }
      context.parent ! ControllerMessage.ReportState(ControllerState.Ready)
      context.become(ready)
      if (this.statisticsUpdateIntervalMs.nonEmpty) {
        statusUpdateAskHandle = context.system.scheduler.schedule(
          0.milliseconds,
          FiniteDuration.apply(statisticsUpdateIntervalMs.get, MILLISECONDS),
          self,
          QueryStatistics
        )
      }
      unstashAll()
    } else {
      val next = frontier.filter(i =>
        workflow
          .inLinks(i)
          .forall(x => operatorStateMap.contains(x) && operatorStateMap(x) == PrincipalState.Ready)
      )
      frontier --= next
      val prevInfo = next
        .flatMap(
          workflow
            .inLinks(_)
            .map(x =>
              (
                workflow.operators(x),
                operatorToWorkerLayers(x).last.clone().asInstanceOf[WorkerLayer]
              )
            )
        )
        .toArray
      val nodes = availableNodes
      val operatorsToWait = new ArrayBuffer[OperatorIdentifier]
      for (k <- next) {
        if (withCheckpoint) {
          for (n <- workflow.outLinks(k)) {
            if (workflow.operators(n).requiredShuffle) {
              operatorsToWait.append(k)
              linksToIgnore.add((k, n))
            }
          }
        }
        val v = workflow.operators(k)
        v.runtimeCheck(workflow) match {
          case Some(dependencies) =>
            dependencies.foreach { x =>
              if (startDependencies.contains(x._1)) {
                startDependencies(x._1) ++= x._2
              } else {
                startDependencies.put(x._1, x._2)
              }
            }
          case None =>
        }
        initializeOperatorDataStructures(k)
        initializeOperators(k, prevInfo)

        for (from <- workflow.inLinks(k)) {
          if (!linksToIgnore.contains(from, k)) {
            val edge = new OperatorLink(
              (
                workflow.operators(from),
                operatorToWorkerLayers(from).last.clone().asInstanceOf[WorkerLayer]
              ),
              (
                workflow.operators(k),
                operatorToWorkerLayers(k).head.clone().asInstanceOf[WorkerLayer]
              )
            )
            edge.link()
            edges(edge.tag) = edge
          }
        }
      }
      next --= operatorsToWait
      frontier ++= next.filter(workflow.outLinks.contains).flatMap(workflow.outLinks(_))
      stashedFrontier ++= operatorsToWait
        .filter(workflow.outLinks.contains)
        .flatMap(workflow.outLinks(_))
      frontier --= stashedFrontier
    }
  }

  private def areAllWorkersCompleted(opIdentifier: OperatorIdentifier): Boolean = {
    operatorToWorkerStateMap(opIdentifier).values.forall(_ == WorkerState.Completed)
  }

  private def unCompletedWorkerStates(
      opIdentifier: OperatorIdentifier
  ): Iterable[WorkerState.Value] = {
    operatorToWorkerStateMap(opIdentifier).filter(x => x._2 != WorkerState.Completed).values
  }

  private def handleWorkerStateReportsInRunning(
      state: WorkerState.Value
  ): Unit = {
    val opId = workerToOperator(sender)
    if (setWorkerState(sender, state)) {
      state match {
        case WorkerState.Paused =>
          if (areAllWorkersCompleted(workerToOperator(sender))) {
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            operatorStateMap(workerToOperator(sender)) = PrincipalState.Completed
            matchNewOperatorStateAndTakeActionsInRunning(workerToOperator(sender))
          } else if (
            whenAllUncompletedWorkersBecome(workerToOperator(sender), WorkerState.Paused)
          ) {
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            operatorStateMap(workerToOperator(sender)) = PrincipalState.Paused
            matchNewOperatorStateAndTakeActionsInRunning(workerToOperator(sender))
          }
        case WorkerState.Completed =>
          if (areAllWorkersCompleted(workerToOperator(sender))) {
            if (operatorToTimer(workerToOperator(sender)).isRunning) {
              operatorToTimer(workerToOperator(sender)).stop()
            }
            controllerLogger.logInfo(
              workflow
                .operators(workerToOperator(sender))
                .tag
                .toString + " completed! Time Elapsed: " + timer.toString()
            )
            operatorStateMap(workerToOperator(sender)) = PrincipalState.Completed
            matchNewOperatorStateAndTakeActionsInRunning(workerToOperator(sender))
          }
        case _ => // skip others
      }
    }
  }

  private def handleWorkerStateReportsInPausing(state: WorkerState.Value): Unit = {
    controllerLogger.logInfo("pausing: " + sender + " to " + state)
    val opId = workerToOperator(sender)
    if (setWorkerState(sender, state)) {
      if (areAllWorkersCompleted(workerToOperator(sender))) {
        safeRemoveAskOperatorHandle(workerToOperator(sender))
        operatorStateMap(workerToOperator(sender)) = PrincipalState.Completed
        matchNewOperatorStateAndTakeActionsInPausing(workerToOperator(sender))
      } else if (whenAllUncompletedWorkersBecome(workerToOperator(sender), WorkerState.Paused)) {
        safeRemoveAskOperatorHandle(workerToOperator(sender))
        if (this.eventListener.reportCurrentTuplesListener != null) {
          this.eventListener.reportCurrentTuplesListener.apply(
            ReportCurrentProcessingTuple(
              workflow.operators(workerToOperator(sender)).tag.operator,
              operatorToReceivedTuples(workerToOperator(sender)).toArray
            )
          );
        }
        operatorToReceivedTuples(workerToOperator(sender)).clear()
        operatorStateMap(workerToOperator(sender)) = PrincipalState.Paused
        matchNewOperatorStateAndTakeActionsInPausing(workerToOperator(sender))
      }
    }
  }

  private def matchNewOperatorStateAndTakeActionsInRunning(
      opIdentifier: OperatorIdentifier
  ): Unit = {
    operatorStateMap(opIdentifier) match {
      case PrincipalState.Completed =>
        controllerLogger.logInfo(sender + " completed")
        if (stashedNodes.contains(opIdentifier)) {
          operatorToWorkerLayers(opIdentifier).foreach { x =>
            x.layer.foreach { worker =>
              AdvancedMessageSending.nonBlockingAskWithRetry(worker, ReleaseOutput, 10, 0)
            }
          }
          stashedNodes.remove(opIdentifier)
        }
        if (operatorStateMap.values.forall(_ == PrincipalState.Completed)) {
          timer.stop()
          controllerLogger.logInfo("workflow completed! Time Elapsed: " + timer.toString())
          timer.reset()
          safeRemoveAskHandle()
          if (frontier.isEmpty) {
            context.parent ! ControllerMessage.ReportState(ControllerState.Completed)
            context.become(completed)
            // collect all output results back to controller
            val sinkOperatorIdentifiers = this.workflow.endOperators
            for (sinkOperator <- sinkOperatorIdentifiers) {
              operatorToWorkerLayers(sinkOperator).foreach { x =>
                x.layer.foreach { worker => worker ! CollectSinkResults }
              }
            }
            if (this.statusUpdateAskHandle != null) {
              this.statusUpdateAskHandle.cancel()
              self ! QueryStatistics
            }
            //              self ! PoisonPill
          } else {
            recoveryMode = false
            operatorInCurrentStage.clear()
            context.become(receive)
            self ! ContinuedInitialization
          }
        }
      case PrincipalState.Paused =>
        if (operatorStateMap.values.forall(_ == PrincipalState.Completed)) {
          if (timer.isRunning) {
            timer.stop()
          }
          controllerLogger.logInfo("workflow completed! Time Elapsed: " + timer.toString())
          timer.reset()
          safeRemoveAskHandle()
          context.parent ! ControllerMessage.ReportState(ControllerState.Completed)
          context.become(completed)
        } else if (allUnCompletedOperatorStates.forall(_ == PrincipalState.Paused)) {
          if (pauseTimer.isRunning) {
            pauseTimer.stop()
          }
          controllerLogger.logInfo("workflow paused! Time Elapsed: " + pauseTimer.toString())
          pauseTimer.reset()
          safeRemoveAskHandle()
          context.parent ! ControllerMessage.ReportState(ControllerState.Paused)
          if (this.eventListener.workflowPausedListener != null) {
            this.eventListener.workflowPausedListener.apply(WorkflowPaused())
          }
          context.become(paused)
        }
      case _ => //skip others
    }
  }

  private def matchNewOperatorStateAndTakeActionsInPausing(
      opIdentifier: OperatorIdentifier
  ): Unit = {
    if (
      operatorStateMap(opIdentifier) != PrincipalState.Paused && operatorStateMap(
        opIdentifier
      ) != PrincipalState.Pausing && operatorStateMap(opIdentifier) != PrincipalState.Completed
    ) {
      operatorToWorkerLayers(opIdentifier).foreach(l => {
        l.layer.foreach(worker => worker ! Pause)
      })
    } else {
      if (operatorStateMap.values.forall(_ == PrincipalState.Completed)) {
        timer.stop()
        frontier.clear()
        controllerLogger.logInfo("workflow completed! Time Elapsed: " + timer.toString())
        timer.reset()
        safeRemoveAskHandle()
        if (frontier.isEmpty) {
          context.parent ! ControllerMessage.ReportState(ControllerState.Completed)
          context.become(completed)
        } else {
          context.become(receive)
          self ! ContinuedInitialization
        }
        unstashAll()
      } else if (allUnCompletedOperatorStates.forall(_ == PrincipalState.Paused)) {
        if (pauseTimer.isRunning) {
          pauseTimer.stop()
        }
        frontier.clear()
        controllerLogger.logInfo("workflow paused! Time Elapsed: " + pauseTimer.toString())
        pauseTimer.reset()
        safeRemoveAskHandle()
        context.parent ! ControllerMessage.ReportState(ControllerState.Paused)
        if (this.eventListener.workflowPausedListener != null) {
          this.eventListener.workflowPausedListener.apply(WorkflowPaused())
        }
        context.become(paused)
        unstashAll()
      } else {
        val next = frontier.filter(i =>
          workflow
            .inLinks(i)
            .map(x => operatorStateMap(x))
            .forall(x => x == PrincipalState.Paused || x == PrincipalState.Completed)
        )
        frontier --= next
        next.foreach(opId => {
          operatorToWorkerLayers(opId).foreach(l => {
            l.layer.foreach(worker => worker ! Pause)
          })
        })
        frontier ++= next.filter(workflow.outLinks.contains).flatMap(workflow.outLinks(_))
      }
    }
  }

  final lazy val allowedStatesOnPausing: Set[WorkerState.Value] =
    Set(WorkerState.Completed, WorkerState.Paused, WorkerState.LocalBreakpointTriggered)

  final lazy val allowedStatesOnResuming: Set[WorkerState.Value] =
    Set(WorkerState.Running, WorkerState.Ready, WorkerState.Completed)

  override def receive: Receive = {
    disallowActorRefRelatedMessages orElse {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
      case QueryStatistics =>
      // do nothing, not initialized yet
      case EnforceStateCheck(operatorIdentifier) =>
        for ((k, v) <- operatorToWorkerStateMap(operatorIdentifier)) {
          if (v != WorkerState.Ready) {
            k ! QueryState
          }
        }
      case WorkerMessage.ReportState(state) =>
        if (state != WorkerState.Ready) {
          sender ! AckedWorkerInitialization()
        } else if (setWorkerState(sender, state)) {
          val opIdentifier = workerToOperator(sender)
          if (whenAllUncompletedWorkersBecome(opIdentifier, WorkerState.Ready)) {
            safeRemoveAskOperatorHandle(opIdentifier)
            operatorToWorkerEdges(opIdentifier).foreach(x => x.link())
            operatorStateMap(opIdentifier) = PrincipalState.Ready
            initializingNextFrontier()
          }
        }
      case AckedControllerInitialization =>
        val nodes = availableNodes
        controllerLogger.logInfo(
          "start initialization --------cluster have " + nodes.length + " nodes---------"
        )
        for (k <- workflow.startOperators) {
          initializeOperatorDataStructures(k)
        }
        workflow.startOperators.foreach(startOp =>
          initializeOperators(startOp, Array[(OpExecConfig, WorkerLayer)]())
        )
        frontier ++= workflow.startOperators.flatMap(workflow.outLinks(_))
      case ContinuedInitialization =>
        controllerLogger.logInfo("continue initialization")
        prevFrontier = frontier
        val nodes = availableNodes
        for (k <- frontier) {
          val v = workflow.operators(k)
          initializeOperatorDataStructures(k)
        }
        frontier.foreach { x =>
          initializeOperators(x, Array())
        }
        val next = frontier.flatMap(workflow.outLinks(_))
        frontier.clear()
        frontier ++= next

      case msg => stash()
    }
  }

  private[this] def ready: Receive = {
    disallowActorRefRelatedMessages orElse {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
        eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
      case QueryStatistics =>
        operatorToWorkerLayers.keys.foreach(opIdentifier => {
          operatorToWorkerLayers(opIdentifier).foreach(l => {
            l.layer.foreach(worker => {
              worker ! QueryStatistics
            })
          })
        })
      case WorkerMessage.ReportStatistics(statistics) =>
        operatorToWorkerStatisticsMap(workerToOperator(sender))(sender) = statistics
        triggerStatusUpdateEvent();
      case Start =>
        controllerLogger.logInfo("received start signal")
        if (!timer.isRunning) {
          timer.start()
        }
        workflow.startOperators.foreach { x =>
          if (!startDependencies.contains(x)) {
            operatorToWorkerLayers(x).foreach(l => {
              l.layer.foreach(worker => {
                worker ! Start
              })
            })
          }
        }
      case WorkerMessage.ReportState(state) =>
        setWorkerState(sender, state)
        state match {
          case WorkerState.Running =>
            operatorStateMap(workerToOperator(sender)) = PrincipalState.Running
            if (!operatorToTimer(workerToOperator(sender)).isRunning) {
              operatorToTimer(workerToOperator(sender)).start()
            }
            if (!operatorToStage1Timer(workerToOperator(sender)).isRunning) {
              operatorToStage1Timer(workerToOperator(sender)).start()
            }
            controllerLogger.logInfo("workflow started!")
            context.parent ! ControllerMessage.ReportState(ControllerState.Running)
            context.become(running)
          // unstashAll()
          case WorkerState.Paused =>
            if (whenAllUncompletedWorkersBecome(workerToOperator(sender), WorkerState.Paused)) {
              // operator paused
              safeRemoveAskOperatorHandle(workerToOperator(sender))
              operatorStateMap(workerToOperator(sender)) = PrincipalState.Paused
              if (operatorStateMap.values.forall(_ == PrincipalState.Paused)) {
                // workflow paused
                pauseTimer.stop()
                controllerLogger.logInfo("workflow paused! Time Elapsed: " + pauseTimer.toString())
                pauseTimer.reset()
                safeRemoveAskHandle()
                context.parent ! ControllerMessage.ReportState(ControllerState.Paused)
                if (this.eventListener.workflowPausedListener != null) {
                  this.eventListener.workflowPausedListener.apply(WorkflowPaused())
                }
                context.become(paused)
              } else if (operatorStateMap.values.forall(_ == PrincipalState.Completed)) {
                // workflow is already complete
                timer.stop()
                controllerLogger.logInfo("workflow completed! Time Elapsed: " + timer.toString())
                timer.reset()
                safeRemoveAskHandle()
                context.parent ! ControllerMessage.ReportState(ControllerState.Completed)
                context.become(completed)
              }
            }
          case _ => //throw new AmberException("Invalid worker state received!")
        }
      case PassBreakpointTo(id: String, breakpoint: GlobalBreakpoint) =>
      //TODO: invoke breakpoint assignment
      case msg =>
        controllerLogger.logInfo("Stashing: " + msg)
        stash()
    }
  }

  private[this] def running: Receive = {
    disallowActorRefRelatedMessages orElse [Any, Unit] {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
      case QueryStatistics =>
        operatorToWorkerLayers.keys.foreach(opIdentifier => {
          operatorToWorkerLayers(opIdentifier).foreach(l => {
            l.layer.foreach(worker => {
              worker ! QueryStatistics
            })
          })
        })
      case WorkerMessage.ReportStatistics(statistics) =>
        operatorToWorkerStatisticsMap(workerToOperator(sender))(sender) = statistics
        triggerStatusUpdateEvent();
      case EnforceStateCheck(operatorIdentifier) =>
        operatorStateMap(operatorIdentifier) match {
          case PrincipalState.Pausing =>
            for ((k, v) <- operatorToWorkerStateMap(operatorIdentifier)) {
              if (!allowedStatesOnPausing.contains(v)) {
                k ! QueryState
              }
            }
          case _ =>
          // shouldn't reach here. there is no state check when workflow is simply running
        }
      case WorkerMessage.ReportState(state) =>
        controllerLogger.logInfo("running: " + sender + " to " + state)
        if (state == WorkerState.Running) {
          operatorStateMap(workerToOperator(sender)) = PrincipalState.Running
        }
        operatorStateMap(workerToOperator(sender)) match {
          case PrincipalState.Pausing =>
            handleWorkerStateReportsInPausing(state)
          case _ =>
            handleWorkerStateReportsInRunning(state)
        }
      case Pause =>
        pauseTimer.start()
        if (sender != self) {
          operatorToIsUserPaused.keys.foreach(k => operatorToIsUserPaused(k) = true)
        }
        operatorToWorkerLayers.keys.foreach(opId => {
          operatorToWorkerLayers(opId).foreach(layer => {
            layer.layer.foreach(worker => worker ! Pause)
          })
          safeRemoveAskOperatorHandle(opId)
          operatorToPeriodicallyAskHandle(opId) =
            context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck(opId))
        })

        controllerLogger.logInfo("received pause signal")
        safeRemoveAskHandle()
        // periodicallyAskHandle =
        // context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
        context.parent ! ControllerMessage.ReportState(ControllerState.Pausing)
        context.become(pausing)
      case ReportWorkerPartialCompleted(workerTag, layer) =>
        sender ! Ack
        if (operatorToLayerCompletedCounter(workerToOperator(sender)).contains(layer)) {
          operatorToLayerCompletedCounter(workerToOperator(sender))(layer) -= 1

          if (operatorToLayerCompletedCounter(workerToOperator(sender))(layer) == 0) {
            // all dependencies for the operator are done
            operatorToLayerCompletedCounter(workerToOperator(sender)) -= layer
            for (i <- startDependencies.keys) {
              if (
                startDependencies(i)
                  .contains(workerToOperator(sender)) && startDependencies(i)(
                  workerToOperator(sender)
                ).contains(layer)
              ) {
                startDependencies(i)(workerToOperator(sender)) -= layer
                if (startDependencies(i)(workerToOperator(sender)).isEmpty) {
                  startDependencies(i) -= workerToOperator(sender)
                  if (startDependencies(i).isEmpty) {
                    startDependencies -= i
                    operatorToWorkerLayers(i.asInstanceOf[OperatorIdentifier]).foreach(l => {
                      l.layer.foreach(worker => worker ! Start)
                    })
                  }
                }
              }
            }
          }
        }
      case Resume =>
      case msg    => stash()
    }
  }

  private[this] def pausing: Receive = {
    disallowActorRefRelatedMessages orElse [Any, Unit] {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
      case QueryStatistics =>
        operatorToWorkerLayers.keys.foreach(opIdentifier => {
          operatorToWorkerLayers(opIdentifier).foreach(l => {
            l.layer.foreach(worker => {
              worker ! QueryStatistics
            })
          })
        })
      case WorkerMessage.ReportStatistics(statistics) =>
        operatorToWorkerStatisticsMap(workerToOperator(sender))(sender) = statistics
        triggerStatusUpdateEvent();
      case reportCurrentTuple: WorkerMessage.ReportCurrentProcessingTuple =>
        operatorToReceivedTuples(workerToOperator(sender)).append(
          (reportCurrentTuple.tuple, reportCurrentTuple.workerID)
        )
      case EnforceStateCheck(operatorIdentifier) =>
        operatorStateMap(operatorIdentifier) match {
          case _ =>
            for ((k, v) <- operatorToWorkerStateMap(operatorIdentifier)) {
              if (!allowedStatesOnPausing.contains(v)) {
                k ! QueryState
              }
            }
        }

      case WorkerMessage.ReportState(state) =>
        operatorStateMap(workerToOperator(sender)) match {
          case _ =>
            handleWorkerStateReportsInPausing(state)
        }
      case Pause => // do nothing
      case msg   => stash()
    }
  }

  private[this] def paused: Receive = {
    disallowActorRefRelatedMessages orElse {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
      case QueryStatistics =>
        operatorToWorkerLayers.keys.foreach(opIdentifier => {
          operatorToWorkerLayers(opIdentifier).foreach(l => {
            l.layer.foreach(worker => {
              worker ! QueryStatistics
            })
          })
        })
      case WorkerMessage.ReportStatistics(statistics) =>
        operatorToWorkerStatisticsMap(workerToOperator(sender))(sender) = statistics
        triggerStatusUpdateEvent();
      case Resume =>
        operatorToIsUserPaused.keys.foreach(opId => operatorToIsUserPaused(opId) = false) //reset
        operatorToWorkerStateMap.keys.foreach(opId => {
          operatorToWorkerStateMap(opId)
            .filter(x => x._2 != WorkerState.Completed)
            .keys
            .foreach(worker => worker ! Resume)
          safeRemoveAskOperatorHandle(opId)
          operatorToPeriodicallyAskHandle(opId) =
            context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck(opId))
        })
        frontier ++= workflow.endOperators.flatMap(workflow.inLinks(_))
        controllerLogger.logInfo("received resume signal")
        safeRemoveAskHandle()

        context.parent ! ControllerMessage.ReportState(ControllerState.Resuming)
        context.become(resuming)
        unstashAll()
      case Pause                    =>
      case EnforceStateCheck        =>
      case ModifyLogic(newMetadata) =>
        // newLogic is now an OperatorMetadata

        controllerLogger.logInfo("modify logic received by controller, sending to principal")
        operatorToWorkerLayers(newMetadata.tag).foreach(l => {
          l.layer.foreach(worker => worker ! ModifyLogic(newMetadata))
        })
        controllerLogger.logInfo("modify logic received by controller, sent to principal")
        context.parent ! Ack
        if (this.eventListener.modifyLogicCompletedListener != null) {
          this.eventListener.modifyLogicCompletedListener.apply(ModifyLogicCompleted())
        }
      case SkipTupleGivenWorkerRef(actorPath, faultedTuple) =>
        val actorRefFuture = this.context.actorSelection(actorPath).resolveOne()
        actorRefFuture.onComplete {
          case scala.util.Success(actorRef) =>
            AdvancedMessageSending.blockingAskWithRetry(actorRef, SkipTuple(faultedTuple), 5)
            if (this.eventListener.skipTupleResponseListener != null) {
              this.eventListener.skipTupleResponseListener.apply(SkipTupleResponse())
            }
          case scala.util.Failure(t) =>
            throw t
        }
      case msg => stash()
    }
  }

  private[this] def resuming: Receive = {
    disallowActorRefRelatedMessages orElse {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
      case QueryStatistics =>
        operatorToWorkerLayers.keys.foreach(opIdentifier => {
          operatorToWorkerLayers(opIdentifier).foreach(l => {
            l.layer.foreach(worker => {
              worker ! QueryStatistics
            })
          })
        })
      case WorkerMessage.ReportStatistics(statistics) =>
        operatorToWorkerStatisticsMap(workerToOperator(sender))(sender) = statistics
        triggerStatusUpdateEvent();
      case EnforceStateCheck =>
        frontier
          .flatMap(workflow.outLinks(_))
          .foreach(opID => {
            for ((k, v) <- operatorToWorkerStateMap(opID)) {
              if (!allowedStatesOnResuming.contains(v)) {
                k ! QueryState
              }
            }
          })
      case WorkerMessage.ReportState(state) =>
        if (!allowedStatesOnResuming.contains(state)) {
          sender ! Resume
        } else if (setWorkerState(sender, state)) {
          if (areAllWorkersCompleted(workerToOperator(sender))) {
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            operatorStateMap(workerToOperator(sender)) = PrincipalState.Completed
          } else if (
            operatorToWorkerStateMap(workerToOperator(sender)).values.forall(
              _ != WorkerState.Paused
            )
          ) {
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            if (
              operatorToWorkerStateMap(workerToOperator(sender)).values.exists(
                _ != WorkerState.Ready
              )
            ) {
              operatorStateMap(workerToOperator(sender)) = PrincipalState.Running
            } else {
              operatorStateMap(workerToOperator(sender)) = PrincipalState.Ready
            }
          }
        }
        if (operatorStateMap.values.forall(_ != PrincipalState.Paused)) {
          frontier.clear()
          if (operatorStateMap.values.exists(_ != PrincipalState.Ready)) {
            controllerLogger.logInfo("workflow resumed!")
            safeRemoveAskHandle()
            context.parent ! ControllerMessage.ReportState(ControllerState.Running)
            context.become(running)

          } else {
            controllerLogger.logInfo("workflow ready!")
            safeRemoveAskHandle()
            context.parent ! ControllerMessage.ReportState(ControllerState.Ready)
            context.become(ready)
            unstashAll()
          }
        } else {
          val next = frontier.filter(i =>
            !workflow
              .outLinks(i)
              .map(x => operatorStateMap(x))
              .contains(PrincipalState.Paused)
          )
          frontier --= next
          next.foreach(opId => {
            operatorToWorkerLayers(opId).foreach(l => {
              l.layer.foreach(worker => worker ! Resume)
            })
          })
          frontier ++= next.filter(workflow.inLinks.contains).flatMap(workflow.inLinks(_))

        }
      case msg => stash()
    }
  }

  private[this] def completed: Receive = {
    disallowActorRefRelatedMessages orElse {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        controllerLogger.logError(err)
      case QueryStatistics =>
        operatorToWorkerLayers.keys.foreach(opIdentifier => {
          operatorToWorkerLayers(opIdentifier).foreach(l => {
            l.layer.foreach(worker => {
              worker ! QueryStatistics
            })
          })
        })
      case WorkerMessage.ReportStatistics(statistics) =>
        operatorToWorkerStatisticsMap(workerToOperator(sender))(sender) = statistics
        triggerStatusUpdateEvent();
      case WorkerMessage.ReportOutputResult(sinkResult) =>
        operatorToWorkerSinkResultMap(workerToOperator(sender))(sender) = sinkResult
        if (
          operatorToWorkerSinkResultMap(workerToOperator(sender)).size == operatorToWorkerStateMap(
            workerToOperator(sender)
          ).keys.size
        ) {
          val collectedResults = mutable.MutableList[ITuple]()
          this
            .operatorToWorkerSinkResultMap(workerToOperator(sender))
            .values
            .foreach(v => collectedResults ++= v)
          operatorToPrincipalSinkResultMap(workerToOperator(sender).operator) =
            collectedResults.toList
          if (operatorToPrincipalSinkResultMap.size == this.workflow.endOperators.size) {
            if (this.eventListener.workflowCompletedListener != null) {
              this.eventListener.workflowCompletedListener
                .apply(WorkflowCompleted(this.operatorToPrincipalSinkResultMap.toMap))
            }
          }
        }
        this.exitIfCompleted
      case msg =>
        controllerLogger.logInfo(s"received: $msg after workflow completed!")

        this.exitIfCompleted
    }
  }

  private[this] def exitIfCompleted: Unit = {
    val reportStatistics = this.eventListener.workflowStatusUpdateListener != null;
    val reportOutputResult = this.eventListener.workflowCompletedListener != null;

    val reportStatisticsCompleted = !reportStatistics ||
      this.operatorStateMap.values.forall(v => v == PrincipalState.Completed)
    val reportOutputResultCompleted = !reportOutputResult ||
      this.operatorToPrincipalSinkResultMap.size == this.workflow.endOperators.size

    if (reportStatisticsCompleted && reportOutputResultCompleted) {
      self ! PoisonPill;
    }

  }

}
