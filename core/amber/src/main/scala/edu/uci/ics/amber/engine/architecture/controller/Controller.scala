package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ExceptionGlobalBreakpoint,
  GlobalBreakpoint
}
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
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  ActorLayer,
  GeneratorWorkerLayer,
  ProcessorWorkerLayer
}
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
import edu.uci.ics.amber.engine.architecture.principal.{
  Principal,
  PrincipalState,
  PrincipalStatistics
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
) extends WorkflowActor {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

//  val principalBiMap: BiMap[OperatorIdentifier, ActorRef] =
//    HashBiMap.create[OperatorIdentifier, ActorRef]()
//  val principalInCurrentStage = new mutable.HashSet[ActorRef]()
//  val principalStates = new mutable.AnyRefMap[ActorRef, PrincipalState.Value]
//  val principalStatisticsMap = new mutable.AnyRefMap[ActorRef, PrincipalStatistics]
  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    Logger(
      s"Controller-${tag.getGlobalIdentity}-Logger"
    ).error(err.convertToMap().mkString(" | "))
    eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
  }
  val errorLogger = WorkflowLogger(errorLogAction)
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

//  def allPrincipals: Iterable[ActorRef] = principalStates.keys
//  def unCompletedPrincipals: Iterable[ActorRef] =
//    principalStates.filter(x => x._2 != PrincipalState.Completed).keys
//  def allUnCompletedPrincipalStates: Iterable[PrincipalState.Value] =
//    principalStates.filter(x => x._2 != PrincipalState.Completed).values

  def allUnCompletedOperatorStates: Iterable[PrincipalState.Value] =
    operatorStateMap.filter(x => x._2 != PrincipalState.Completed).values
  val tau: FiniteDuration = Constants.defaultTau
  var operatorToPrincipalSinkResultMap = new mutable.HashMap[String, List[ITuple]]
  var operatorToWorkerLayers = new mutable.HashMap[OperatorIdentifier, Array[ActorLayer]]()
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
  var operatorToWorkersTriggeredBreakpoint =
    new mutable.HashMap[OperatorIdentifier, Iterable[ActorRef]]()
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
//  def getPrincipalNode(nodes: Array[Address]): Address =
//    self.path.address //nodes(util.Random.nextInt(nodes.length))

  private def queryExecuteStatistics(): Unit = {}

  //if checkpoint activated:
  private def insertCheckpoint(from: OpExecConfig, to: OpExecConfig): Unit = {
    //insert checkpoint barrier between 2 operators and delete the link between them
    val topology = from.topology
    val hashFunc = to.getShuffleHashFunction(topology.layers.last.tag)
    val layerTag = LayerTag(from.tag, "checkpoint")
    val path: String = layerTag.getGlobalIdentity
    val numWorkers = topology.layers.last.numWorkers
    val scanGen: Int => ISourceOperatorExecutor = i =>
      new HDFSFolderScanSourceOperatorExecutor(Constants.remoteHDFSPath, path + "/" + i, '|', null)
    val lastLayer = topology.layers.last
    val materializerLayer = new ProcessorWorkerLayer(
      layerTag,
      i => new HashBasedMaterializer(path, i, hashFunc, numWorkers),
      numWorkers,
      FollowPrevious(),
      OneOnEach()
    )
    topology.layers :+= materializerLayer
    topology.links :+= new LocalPartialToOne(
      lastLayer,
      materializerLayer,
      Constants.defaultBatchSize,
      0
    )
    val scanLayer = new GeneratorWorkerLayer(
      LayerTag(to.tag, "from_checkpoint"),
      scanGen,
      topology.layers.last.numWorkers,
      FollowPrevious(),
      OneOnEach()
    )
    val firstLayer = to.topology.layers.head
    to.topology.layers +:= scanLayer
    to.topology.links +:= new HashBasedShuffle(
      scanLayer,
      firstLayer,
      Constants.defaultBatchSize,
      hashFunc,
      0
    )

  }

  final def safeRemoveAskHandle(): Unit = {
    if (periodicallyAskHandle != null) {
      periodicallyAskHandle.cancel()
      periodicallyAskHandle = null
    }
  }

  private def killAndRecoverStage(): Unit = {
    val futuresNoSinkScan = operatorInCurrentStage
      .filter(x => x.operator.contains("Sink") && x.operator.contains("Scan"))
      .map { x =>
        operatorStateMap(x) = PrincipalState.Running
        operatorToWorkerLayers(x).foreach { layers =>
          layers.layer(0) ! Reset(
            layers.getFirstMetadata,
            Seq(operatorToReceivedRecoveryInformation(x)(layers.tagForFirst))
          )
          operatorToWorkerStateMap(x)(layers.layer(0)) = WorkerState.Ready
        }
      }
      .asJava
//    val tasksNoSinkScan = Futures.sequence(futuresNoSinkScan, ec)
//    Await.result(tasksNoSinkScan, 5.minutes)
//    Thread.sleep(2000)
//    val futuresScan = principalInCurrentStage
//      .filter(x => principalBiMap.inverse().get(x).operator.contains("Scan"))
//      .map { x =>
//        principalStates(x) = PrincipalState.Running
//        AdvancedMessageSending.nonBlockingAskWithRetry(x, KillAndRecover, 5, 0)(3.minutes, ec, log)
//      }
//      .asJava
//    val tasksScan = Futures.sequence(futuresScan, ec)
//    Await.result(tasksScan, 5.minutes)
//    if (this.eventListener.recoveryStartedListener != null) {
//      this.eventListener.recoveryStartedListener.apply()
//    }
//    context.become(pausing)
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
        log.info(
          "Skipped worker state transition for worker{} from {} to {}",
          worker,
          operatorToWorkerStateMap(operatorIdentifier)(worker),
          state
        )
        operatorToWorkerStateMap(operatorIdentifier)(worker) = state
      } else {
        log.warning(
          "Invalid worker state transition for worker{} from {} to {}",
          worker,
          operatorToWorkerStateMap(operatorIdentifier)(worker),
          state
        )
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
      prev: Array[(OpExecConfig, ActorLayer)]
  ): Unit = {
    var metadata =
      workflow.operators(startOp) // This metadata gets updated at the end of this function
    operatorToWorkerLayers(startOp) = metadata.topology.layers
    operatorToWorkerEdges(startOp) = metadata.topology.links
    val all = availableNodes
    if (operatorToWorkerEdges(startOp).isEmpty) {
      operatorToWorkerLayers(startOp).foreach(x => x.build(prev, all))
    } else {
      val inLinks: Map[ActorLayer, Set[ActorLayer]] =
        operatorToWorkerEdges(startOp).groupBy(x => x.to).map(x => (x._1, x._2.map(_.from).toSet))
      var currentLayer: Iterable[ActorLayer] =
        operatorToWorkerEdges(startOp)
          .filter(x => operatorToWorkerEdges(startOp).forall(_.to != x.from))
          .map(_.from)
      currentLayer.foreach(x => x.build(prev, all))
      currentLayer = inLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (currentLayer.nonEmpty) {
        currentLayer.foreach(x => x.build(inLinks(x).map(y => (null, y)).toArray, all))
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
    // context.become(initializing)
    // unstashAll()
    if (!recoveryMode) {
      val breakpointToAssign = new ExceptionGlobalBreakpoint(
        startOp.operator + "-ExceptionBreakpoint"
      )
      operatorToGlobalBreakpoints(startOp)(breakpointToAssign.id) = breakpointToAssign
      metadata.assignBreakpoint(
        operatorToWorkerLayers(startOp),
        operatorToWorkerStateMap(startOp),
        breakpointToAssign
      )
    }
  }

  private def initializingNextFrontier(): Unit = {
    if (
      operatorStateMap.size == workflow.operators.size && operatorStateMap.values.forall(
        _ != PrincipalState.Uninitialized
      )
    ) {
      frontier.clear()
      if (stashedFrontier.nonEmpty) {
        log.info("partially initialized!")
        frontier ++= stashedFrontier
        stashedFrontier.clear()
      } else {
        log.info("fully initialized!")
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
                operatorToWorkerLayers(x).last.clone().asInstanceOf[ActorLayer]
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
              insertCheckpoint(workflow.operators(k), workflow.operators(n))
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
                operatorToWorkerLayers(from).last.clone().asInstanceOf[ActorLayer]
              ),
              (
                workflow.operators(k),
                operatorToWorkerLayers(k).head.clone().asInstanceOf[ActorLayer]
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
    if (setWorkerState(sender, state)) {
      state match {
        case WorkerState.LocalBreakpointTriggered =>
          if (
            whenAllUncompletedWorkersBecome(
              workerToOperator(sender),
              WorkerState.LocalBreakpointTriggered
            )
          ) {
            //only one worker and it triggered breakpoint
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            operatorToPeriodicallyAskHandle(workerToOperator(sender)) =
              context.system.scheduler.schedule(
                0.milliseconds,
                30.seconds,
                self,
                EnforceStateCheck(workerToOperator(sender))
              )
            operatorToWorkersTriggeredBreakpoint(workerToOperator(sender)) =
              operatorToWorkerStateMap(workerToOperator(sender)).keys
            operatorStateMap(workerToOperator(sender)) = PrincipalState.CollectingBreakpoints
            matchNewOperatorStateAndTakeActionsInRunning(workerToOperator(sender))

          } else {
            //no tau involved since we know a very small tau works best
            if (!operatorToStage2Timer(workerToOperator(sender)).isRunning) {
              operatorToStage2Timer(workerToOperator(sender)).start()
            }
            if (operatorToStage1Timer(workerToOperator(sender)).isRunning) {
              operatorToStage1Timer(workerToOperator(sender)).stop()
            }
            context.system.scheduler
              .scheduleOnce(
                tau,
                () =>
                  operatorToWorkerStateMap(workerToOperator(sender))
                    .filter(x => x._2 != WorkerState.Completed)
                    .keys
                    .foreach(worker => worker ! Pause)
              )
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            operatorToPeriodicallyAskHandle(workerToOperator(sender)) = context.system.scheduler
              .schedule(30.seconds, 30.seconds, self, EnforceStateCheck(workerToOperator(sender)))
          }
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
          } else if (
            unCompletedWorkerStates(workerToOperator(sender))
              .forall(x => x == WorkerState.Paused || x == WorkerState.LocalBreakpointTriggered)
          ) {
            operatorToWorkersTriggeredBreakpoint(workerToOperator(sender)) =
              operatorToWorkerStateMap(workerToOperator(sender))
                .filter(_._2 == WorkerState.LocalBreakpointTriggered)
                .keys
            safeRemoveAskOperatorHandle(workerToOperator(sender))
            operatorToPeriodicallyAskHandle(workerToOperator(sender)) = context.system.scheduler
              .schedule(
                1.milliseconds,
                30.seconds,
                self,
                EnforceStateCheck(workerToOperator(sender))
              )
            operatorStateMap(workerToOperator(sender)) = PrincipalState.CollectingBreakpoints
            matchNewOperatorStateAndTakeActionsInRunning(workerToOperator(sender))
          }
        case WorkerState.Completed =>
          if (areAllWorkersCompleted(workerToOperator(sender))) {
            if (operatorToTimer(workerToOperator(sender)).isRunning) {
              operatorToTimer(workerToOperator(sender)).stop()
            }
            log.info(
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

  private def handleWorkerStateReportsInCollBreakpoints(state: WorkerState.Value): Unit = {
    log.info("collecting: " + sender + " to " + state)
    if (setWorkerState(sender, state)) {
      if (unCompletedWorkerStates(workerToOperator(sender)).forall(_ == WorkerState.Paused)) {
        //all breakpoint resolved, it's safe to report to controller and then Pause(on triggered, or user paused) else Resume
        val map = new mutable.HashMap[(ActorRef, FaultedTuple), ArrayBuffer[String]]
        for (
          i <- operatorToGlobalBreakpoints(workerToOperator(sender)).values.filter(_.isTriggered)
        ) {
          operatorToIsUserPaused(workerToOperator(sender)) = true //upgrade pause
          i.report(map)
        }
        safeRemoveAskOperatorHandle(workerToOperator(sender))
        if (!operatorToIsUserPaused(workerToOperator(sender))) {
          log.info("no global breakpoint triggered, continue")
          operatorToIsUserPaused(workerToOperator(sender)) = false //reset
          assert(
            operatorToWorkerStateMap(workerToOperator(sender))
              .filter(x => x._2 != WorkerState.Completed)
              .values
              .nonEmpty
          )
          operatorToWorkerStateMap(workerToOperator(sender))
            .filter(x => x._2 != WorkerState.Completed)
            .keys
            .foreach(worker => worker ! Resume)
          safeRemoveAskOperatorHandle(workerToOperator(sender))
          operatorToPeriodicallyAskHandle(workerToOperator(sender)) = context.system.scheduler
            .schedule(30.seconds, 30.seconds, self, EnforceStateCheck(workerToOperator(sender)))
        } else {
          self ! Pause
          context.parent ! ReportGlobalBreakpointTriggered(
            map,
            workflow.operators(workerToOperator(sender)).tag.operator
          )
          if (this.eventListener.breakpointTriggeredListener != null) {
            this.eventListener.breakpointTriggeredListener.apply(
              BreakpointTriggered(map, workflow.operators(workerToOperator(sender)).tag.operator)
            )
          }
          log.info(map.toString())
          operatorStateMap(workerToOperator(sender)) = PrincipalState.Paused
          log.info(
            "user paused or global breakpoint triggered, pause. Stage1 cost = " + operatorToStage1Timer(
              workerToOperator(sender)
            )
              .toString() + " Stage2 cost =" + operatorToStage2Timer(workerToOperator(sender))
              .toString()
          )
        }
        if (operatorToStage2Timer(workerToOperator(sender)).isRunning) {
          operatorToStage2Timer(workerToOperator(sender)).stop()
        }
        if (!operatorToStage1Timer(workerToOperator(sender)).isRunning) {
          operatorToStage1Timer(workerToOperator(sender)).start()
        }
      }
    }
  }

  private[this] def handleBreakpointOnlyWorkerMessages: Receive = {
    case ReportedTriggeredBreakpoints(bps) =>
      bps.foreach(x => {
        val bp = operatorToGlobalBreakpoints(workerToOperator(sender))(x.id)
        bp.accept(sender, x)
        if (bp.needCollecting) {
          //is not fully collected
          bp.collect()
        } else if (bp.isRepartitionRequired) {
          //fully collected, but need repartition (e.g. count not reach target number)
          //OR need Reset
          workflow
            .operators(workerToOperator(sender))
            .assignBreakpoint(
              operatorToWorkerLayers(workerToOperator(sender)),
              operatorToWorkerStateMap(workerToOperator(sender)),
              bp
            )
        } else if (bp.isCompleted) {
          //fully collected and reach the target
          bp.remove()
        }
      })
    case ReportedQueriedBreakpoint(bp) =>
      val gbp = operatorToGlobalBreakpoints(workerToOperator(sender))(bp.id)
      if (gbp.accept(sender, bp) && !gbp.needCollecting) {
        if (gbp.isRepartitionRequired) {
          //fully collected, but need repartition (count not reach target number)
          workflow
            .operators(workerToOperator(sender))
            .assignBreakpoint(
              operatorToWorkerLayers(workerToOperator(sender)),
              operatorToWorkerStateMap(workerToOperator(sender)),
              gbp
            )
        } else if (gbp.isCompleted) {
          //fully collected and reach the target
          gbp.remove()
        }
      }
  }

  private def matchNewOperatorStateAndTakeActionsInRunning(
      opIdentifier: OperatorIdentifier
  ): Unit = {
    operatorStateMap(opIdentifier) match {
      case PrincipalState.Completed =>
        log.info(sender + " completed")
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
          log.info("workflow completed! Time Elapsed: " + timer.toString())
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
      case PrincipalState.CollectingBreakpoints =>
      case PrincipalState.Paused =>
        if (operatorStateMap.values.forall(_ == PrincipalState.Completed)) {
          if (timer.isRunning) {
            timer.stop()
          }
          log.info("workflow completed! Time Elapsed: " + timer.toString())
          timer.reset()
          safeRemoveAskHandle()
          context.parent ! ControllerMessage.ReportState(ControllerState.Completed)
          context.become(completed)
        } else if (allUnCompletedOperatorStates.forall(_ == PrincipalState.Paused)) {
          if (pauseTimer.isRunning) {
            pauseTimer.stop()
          }
          log.info("workflow paused! Time Elapsed: " + pauseTimer.toString())
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
        log.info("workflow completed! Time Elapsed: " + timer.toString())
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
        log.info("workflow paused! Time Elapsed: " + pauseTimer.toString())
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
    case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
      log.error(err.convertToMap().mkString(" | "))
      eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
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
          operatorToGlobalBreakpoints(opIdentifier).values.foreach(
            workflow
              .operators(opIdentifier)
              .assignBreakpoint(
                operatorToWorkerLayers(opIdentifier),
                operatorToWorkerStateMap(opIdentifier),
                _
              )
          )
          operatorToWorkerStateMap(opIdentifier).keys.foreach(_ ! CheckRecovery)
          operatorStateMap(opIdentifier) = PrincipalState.Ready
          initializingNextFrontier()
        }
      }
    case AckedControllerInitialization =>
      val nodes = availableNodes
      log.info("start initialization --------cluster have " + nodes.length + " nodes---------")
      for (k <- workflow.startOperators) {
        initializeOperatorDataStructures(k)
      }
      workflow.startOperators.foreach(startOp =>
        initializeOperators(startOp, Array[(OpExecConfig, ActorLayer)]())
      )
      frontier ++= workflow.startOperators.flatMap(workflow.outLinks(_))
    case ContinuedInitialization =>
      log.info("continue initialization")
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

  private[this] def ready: Receive = {
    case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
      log.error(err.convertToMap().mkString(" | "))
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
      log.info("received start signal")
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
          log.info("workflow started!")
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
              log.info("workflow paused! Time Elapsed: " + pauseTimer.toString())
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
              log.info("workflow completed! Time Elapsed: " + timer.toString())
              timer.reset()
              safeRemoveAskHandle()
              context.parent ! ControllerMessage.ReportState(ControllerState.Completed)
              context.become(completed)
            }
          }
        case _ => //throw new AmberException("Invalid worker state received!")
      }
    case PassBreakpointTo(id: String, breakpoint: GlobalBreakpoint) =>
      val opTag = OperatorIdentifier(tag, id)
      operatorToGlobalBreakpoints(opTag)(breakpoint.id) = breakpoint
      log.info("assign breakpoint: " + breakpoint.id)
      workflow
        .operators(opTag)
        .assignBreakpoint(
          operatorToWorkerLayers(opTag),
          operatorToWorkerStateMap(opTag),
          breakpoint
        )
    case msg =>
      log.info("Stashing: " + msg)
      stash()
  }

  private[this] def running: Receive = {
    handleBreakpointOnlyWorkerMessages orElse [Any, Unit] {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        log.error(err.convertToMap().mkString(" | "))
        eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
      case KillAndRecover =>
        killAndRecoverStage()
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
          case PrincipalState.CollectingBreakpoints =>
            operatorToWorkersTriggeredBreakpoint(operatorIdentifier).foreach(x =>
              x ! QueryTriggeredBreakpoints
            )
          case _ =>
          // shouldn't reach here. there is no state check when workflow is simply running
        }
      case WorkerMessage.ReportState(state) =>
        log.info("running: " + sender + " to " + state)
        operatorStateMap(workerToOperator(sender)) match {
          case PrincipalState.CollectingBreakpoints =>
            handleWorkerStateReportsInCollBreakpoints(state)
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

        log.info("received pause signal")
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
    handleBreakpointOnlyWorkerMessages orElse [Any, Unit] {
      case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
        log.error(err.convertToMap().mkString(" | "))
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
      case reportCurrentTuple: WorkerMessage.ReportCurrentProcessingTuple =>
        operatorToReceivedTuples(workerToOperator(sender)).append(
          (reportCurrentTuple.tuple, reportCurrentTuple.workerID)
        )
      case EnforceStateCheck(operatorIdentifier) =>
        operatorStateMap(operatorIdentifier) match {
          case PrincipalState.CollectingBreakpoints =>
            operatorToWorkersTriggeredBreakpoint(operatorIdentifier).foreach(x =>
              x ! QueryTriggeredBreakpoints
            )
          case _ =>
            for ((k, v) <- operatorToWorkerStateMap(operatorIdentifier)) {
              if (!allowedStatesOnPausing.contains(v)) {
                k ! QueryState
              }
            }
        }

      case WorkerMessage.ReportState(state) =>
        operatorStateMap(workerToOperator(sender)) match {
          case PrincipalState.CollectingBreakpoints =>
            handleWorkerStateReportsInCollBreakpoints(state)
          case _ =>
            log.info("pausing: " + sender + " to " + state)
            if (setWorkerState(sender, state)) {
              if (areAllWorkersCompleted(workerToOperator(sender))) {
                safeRemoveAskOperatorHandle(workerToOperator(sender))
                operatorStateMap(workerToOperator(sender)) = PrincipalState.Completed
                matchNewOperatorStateAndTakeActionsInPausing(workerToOperator(sender))
              } else if (
                whenAllUncompletedWorkersBecome(workerToOperator(sender), WorkerState.Paused)
              ) {
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
              } else if (
                operatorToWorkerStateMap(workerToOperator(sender))
                  .filter(x => x._2 != WorkerState.Completed)
                  .values
                  .forall(x => x == WorkerState.Paused || x == WorkerState.LocalBreakpointTriggered)
              ) {
                operatorToWorkersTriggeredBreakpoint(workerToOperator(sender)) =
                  operatorToWorkerStateMap(
                    workerToOperator(sender)
                  ).filter(_._2 == WorkerState.LocalBreakpointTriggered).keys
                safeRemoveAskOperatorHandle(workerToOperator(sender))
                operatorToPeriodicallyAskHandle(workerToOperator(sender)) = context.system.scheduler
                  .schedule(
                    1.milliseconds,
                    30.seconds,
                    self,
                    EnforceStateCheck(workerToOperator(sender))
                  )
                operatorStateMap(workerToOperator(sender)) = PrincipalState.CollectingBreakpoints
                matchNewOperatorStateAndTakeActionsInPausing(workerToOperator(sender))
              }
            }
        }
      case Pause => // do nothing
      case msg   => stash()
    }
  }

  private[this] def paused: Receive = {
    case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
      log.error(err.convertToMap().mkString(" | "))
      eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
    case KillAndRecover =>
      killAndRecoverStage()
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
        assert(
          operatorToWorkerStateMap(opId).filter(x => x._2 != WorkerState.Completed).values.nonEmpty
        )
        operatorToWorkerStateMap(opId)
          .filter(x => x._2 != WorkerState.Completed)
          .keys
          .foreach(worker => worker ! Resume)
        safeRemoveAskOperatorHandle(opId)
        operatorToPeriodicallyAskHandle(opId) =
          context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck(opId))
      })
      frontier ++= workflow.endOperators.flatMap(workflow.inLinks(_))
      log.info("received resume signal")
      safeRemoveAskHandle()
      context.parent ! ControllerMessage.ReportState(ControllerState.Resuming)
      context.become(resuming)
      unstashAll()
    case Pause                    =>
    case EnforceStateCheck        =>
    case ModifyLogic(newMetadata) =>
      // newLogic is now an OperatorMetadata
      log.info("modify logic received by controller, sending to principal")
      operatorToWorkerLayers(newMetadata.tag).foreach(l => {
        l.layer.foreach(worker => worker ! ModifyLogic(newMetadata))
      })
      log.info("modify logic received by controller, sent to principal")
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
    case PassBreakpointTo(id: String, breakpoint: GlobalBreakpoint) =>
      val opTag = OperatorIdentifier(tag, id)
      operatorToGlobalBreakpoints(opTag)(breakpoint.id) = breakpoint
      workflow
        .operators(opTag)
        .assignBreakpoint(
          operatorToWorkerLayers(opTag),
          operatorToWorkerStateMap(opTag),
          breakpoint
        )
    case msg => stash()
  }

  private[this] def resuming: Receive = {
    case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
      log.error(err.convertToMap().mkString(" | "))
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
          operatorToWorkerStateMap(workerToOperator(sender)).values.forall(_ != WorkerState.Paused)
        ) {
          safeRemoveAskOperatorHandle(workerToOperator(sender))
          if (
            operatorToWorkerStateMap(workerToOperator(sender)).values.exists(_ != WorkerState.Ready)
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
          log.info("workflow resumed!")
          safeRemoveAskHandle()
          context.parent ! ControllerMessage.ReportState(ControllerState.Running)
          context.become(running)
        } else {
          log.info("workflow ready!")
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

  private[this] def completed: Receive = {
    case LogErrorToFrontEnd(err: WorkflowRuntimeError) =>
      log.error(err.convertToMap().mkString(" | "))
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
      log.info("received: {} after workflow completed!", msg)
      this.exitIfCompleted
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
