package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{ExceptionGlobalBreakpoint, GlobalBreakpoint}
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{BreakpointTriggered, ModifyLogicCompleted, SkipTupleResponse, WorkflowCompleted, WorkflowPaused, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.OneOnEach
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, GeneratorWorkerLayer, ProcessorWorkerLayer}
import edu.uci.ics.amber.engine.faulttolerance.materializer.{HashBasedMaterializer, OutputMaterializer}
import edu.uci.ics.amber.engine.architecture.linksemantics.{FullRoundRobin, HashBasedShuffle, LocalPartialToOne, OperatorLink}
import edu.uci.ics.amber.engine.architecture.principal.{Principal, PrincipalState, PrincipalStatistics}
import edu.uci.ics.amber.engine.common.amberexception.AmberException
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage._
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.PrincipalMessage
import edu.uci.ics.amber.engine.common.ambermessage.PrincipalMessage.{AckedPrincipalInitialization, AssignBreakpoint, GetOutputLayer, ReportCurrentProcessingTuple, ReportOutputResult, ReportPrincipalPartialCompleted}
import edu.uci.ics.amber.engine.common.ambermessage.StateMessage.EnforceStateCheck
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, LinkTag, OperatorIdentifier, WorkflowTag}
import edu.uci.ics.amber.engine.common.tuple.Tuple
import edu.uci.ics.amber.engine.common.{AdvancedMessageSending, AmberUtils, Constants, SourceOperatorExecutor}
import edu.uci.ics.amber.engine.faulttolerance.scanner.HDFSFolderScanSourceOperatorExecutor
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Address, Cancellable, Deploy, PoisonPill, Props, Stash}
import akka.dispatch.Futures
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import com.github.nscala_time.time.Imports.DateTime
import com.google.common.base.Stopwatch
import play.api.libs.json.{JsArray, JsValue, Json}
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object Controller {

  implicit def ord: Ordering[DateTime] = Ordering.by(_.getMillis)

  def props(json: String, withCheckpoint: Boolean = false): Props =
    Props(fromJsonString(json, withCheckpoint))

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

  private def fromJsonString(jsonString: String, withCheckpoint: Boolean): Controller = {
    val json: JsValue = Json.parse(jsonString)
    val tag: WorkflowTag = WorkflowTag("sample")
    val linkArray: JsArray = (json \ "links").as[JsArray]
    val links: Map[OperatorIdentifier, Set[OperatorIdentifier]] =
      linkArray.value
        .map(x =>
          (OperatorIdentifier(tag, x("origin").as[String]), OperatorIdentifier(tag, x("destination").as[String]))
        )
        .groupBy(_._1)
        .map { case (k, v) => (k, v.map(_._2).toSet) }
    val operatorArray: JsArray = (json \ "operators").as[JsArray]
    val operators: mutable.Map[OperatorIdentifier, OpExecConfig] = mutable.Map(
      operatorArray.value.map(x =>
        (OperatorIdentifier(tag, x("operatorID").as[String]), jsonToOperatorMetadata(tag, x))
      ): _*
    )
    new Controller(
      tag,
      new Workflow(operators, links),
      withCheckpoint,
      ControllerEventListener(),
      Option.empty
    )

  }

  private def jsonToOperatorMetadata(workflowTag: WorkflowTag, json: JsValue): OpExecConfig = {
    val id = json("operatorID").as[String]
    val tag = OperatorIdentifier(workflowTag.workflow, id)
//    json("operatorType").as[String] match {
//      case "LocalScanSource" =>
//        new LocalFileScanMetadata(
//          tag,
//          Constants.defaultNumWorkers,
//          json("tableName").as[String],
//          json("delimiter").as[String].charAt(0),
//          (json \ "indicesToKeep").asOpt[Array[Int]].orNull,
//          null
//        )
//      case "HDFSScanSource" =>
//        new HDFSFileScanMetadata(
//          tag,
//          Constants.defaultNumWorkers,
//          json("host").as[String],
//          json("tableName").as[String],
//          json("delimiter").as[String].charAt(0),
//          json("indicesToKeep").asOpt[Array[Int]].orNull,
//          null
//        )
//      case "KeywordMatcher" =>
//        new KeywordSearchMetadata(
//          tag,
//          Constants.defaultNumWorkers,
//          json("attributeName").as[Int],
//          json("keyword").as[String]
//        )
//      case "Aggregation" => new CountMetadata(tag, Constants.defaultNumWorkers)
//      case "Filter" =>
//        new FilterMetadata[DateTime](
//          tag,
//          Constants.defaultNumWorkers,
//          json("targetField").as[Int],
//          FilterType.getType(json("filterType").as[String]),
//          DateTime.parse(json("threshold").as[String])
//        )
//      case "Sink" => new SimpleSinkOperatorMetadata(tag)
//      case "Generate" =>
//        new SimpleSourceOperatorMetadata(
//          tag,
//          Constants.defaultNumWorkers,
//          json("limit").as[Int],
//          json("delay").as[Int]
//        )
//      case "HashJoin" =>
//        new HashJoinMetadata[String](
//          tag,
//          Constants.defaultNumWorkers,
//          json("innerTableIndex").as[Int],
//          json("outerTableIndex").as[Int]
//        )
//      case "GroupBy" =>
//        new GroupByMetadata[String](
//          tag,
//          Constants.defaultNumWorkers,
//          json("groupByField").as[Int],
//          json("aggregateField").as[Int],
//          AggregationType.valueOf(json("aggregationType").as[String])
//        )
//      case "Projection" =>
//        new ProjectionMetadata(
//          tag,
//          Constants.defaultNumWorkers,
//          json("targetFields").as[Array[Int]]
//        )
//      case "Sort" => new SortMetadata[String](tag, json("targetField").as[Int])
//      case t      => throw new NotImplementedError("Unknown operator type: " + t)
//    }
    throw new UnsupportedOperationException()
  }
}

class Controller(
    val tag: WorkflowTag,
    val workflow: Workflow,
    val withCheckpoint: Boolean,
    val eventListener: ControllerEventListener = ControllerEventListener(),
    val statisticsUpdateIntervalMs: Option[Long]
) extends Actor
    with ActorLogging
    with Stash {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

  val principalBiMap: BiMap[OperatorIdentifier, ActorRef] = HashBiMap.create[OperatorIdentifier, ActorRef]()
  val principalInCurrentStage = new mutable.HashSet[ActorRef]()
  val principalStates = new mutable.AnyRefMap[ActorRef, PrincipalState.Value]
  val principalStatisticsMap = new mutable.AnyRefMap[ActorRef, PrincipalStatistics]
  val principalSinkResultMap = new mutable.HashMap[String, List[Tuple]]
  val edges = new mutable.AnyRefMap[LinkTag, OperatorLink]
  val frontier = new mutable.HashSet[OperatorIdentifier]
  var prevFrontier: mutable.HashSet[OperatorIdentifier] = _
  val stashedFrontier = new mutable.HashSet[OperatorIdentifier]
  val stashedNodes = new mutable.HashSet[ActorRef]()
  val linksToIgnore = new mutable.HashSet[(OperatorIdentifier, OperatorIdentifier)]
  var periodicallyAskHandle: Cancellable = _
  var statusUpdateAskHandle: Cancellable = _
  var startDependencies =
    new mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]]
  val timer = Stopwatch.createUnstarted();
  val pauseTimer = Stopwatch.createUnstarted();
  var recoveryMode = false

  def allPrincipals: Iterable[ActorRef] = principalStates.keys
  def unCompletedPrincipals: Iterable[ActorRef] =
    principalStates.filter(x => x._2 != PrincipalState.Completed).keys
  def allUnCompletedPrincipalStates: Iterable[PrincipalState.Value] =
    principalStates.filter(x => x._2 != PrincipalState.Completed).values
  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]
  def getPrincipalNode(nodes: Array[Address]): Address =
    self.path.address //nodes(util.Random.nextInt(nodes.length))

  private def queryExecuteStatistics(): Unit = {}

  //if checkpoint activated:
  private def insertCheckpoint(from: OpExecConfig, to: OpExecConfig): Unit = {
    //insert checkpoint barrier between 2 operators and delete the link between them
    val topology = from.topology
    val hashFunc = to.getShuffleHashFunction(topology.layers.last.tag)
    val layerTag = LayerTag(from.tag, "checkpoint")
    val path: String = layerTag.getGlobalIdentity
    val numWorkers = topology.layers.last.numWorkers
    val scanGen: Int => SourceOperatorExecutor = i =>
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
      Constants.defaultBatchSize
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
      hashFunc
    )

  }

  final def safeRemoveAskHandle(): Unit = {
    if (periodicallyAskHandle != null) {
      periodicallyAskHandle.cancel()
      periodicallyAskHandle = null
    }
  }

  private def killAndRecoverStage(): Unit = {
    val futuresNoSinkScan = principalInCurrentStage
      .filter(x =>
        !principalBiMap.inverse().get(x).operator.contains("Sink") &&
          !principalBiMap.inverse().get(x).operator.contains("Scan")
      )
      .map { x =>
        principalStates(x) = PrincipalState.Running
        AdvancedMessageSending.nonBlockingAskWithRetry(x, KillAndRecover, 5, 0)(3.minutes, ec, log)
      }
      .asJava
    val tasksNoSinkScan = Futures.sequence(futuresNoSinkScan, ec)
    Await.result(tasksNoSinkScan, 5.minutes)
    Thread.sleep(2000)
    val futuresScan = principalInCurrentStage
      .filter(x => principalBiMap.inverse().get(x).operator.contains("Scan"))
      .map { x =>
        principalStates(x) = PrincipalState.Running
        AdvancedMessageSending.nonBlockingAskWithRetry(x, KillAndRecover, 5, 0)(3.minutes, ec, log)
      }
      .asJava
    val tasksScan = Futures.sequence(futuresScan, ec)
    Await.result(tasksScan, 5.minutes)
    if (this.eventListener.recoveryStartedListener != null) {
      this.eventListener.recoveryStartedListener.apply()
    }
    context.become(pausing)
  }

  def triggerStatusUpdateEvent(): Unit = {
    if (
      this.eventListener.workflowStatusUpdateListener != null
      && this.principalStatisticsMap.nonEmpty
    ) {
      val workflowStatus = this.principalStatisticsMap
        .map(e => (this.principalBiMap.inverse().get(e._1).operator, e._2))
        .toMap;
      this.eventListener.workflowStatusUpdateListener.apply(WorkflowStatusUpdate(workflowStatus))
    }
  }

  override def receive: Receive = {
    case QueryStatistics =>
    // do nothing, not initialized yet
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case AckedControllerInitialization =>
      val nodes = availableNodes
      log.info("start initialization --------cluster have " + nodes.length + " nodes---------")
      for (k <- workflow.startOperators) {
        val v = workflow.operators(k)
        val p = context.actorOf(
          Principal.props(v).withDeploy(Deploy(scope = RemoteScope(getPrincipalNode(nodes)))),
          v.tag.operator
        )
        principalBiMap.put(k, p)
        principalStates(p) = PrincipalState.Uninitialized
        principalInCurrentStage.add(p)
      }
      workflow.startOperators.foreach { x =>
        val principal = principalBiMap.get(x)
        AdvancedMessageSending.nonBlockingAskWithRetry(
          principal,
          AckedPrincipalInitialization(Array()),
          10,
          0,
          y =>
            y match {
              case AckWithInformation(z) =>
                workflow.operators(x) = z.asInstanceOf[OpExecConfig]
                //assign exception breakpoint before all breakpoints
                if (!recoveryMode) {
                  AdvancedMessageSending.blockingAskWithRetry(
                    principal,
                    AssignBreakpoint(
                      new ExceptionGlobalBreakpoint(x.operator + "-ExceptionBreakpoint")
                    ),
                    10
                  )
                }
              case other => throw new AmberException("principal didn't return updated metadata")
            }
        )
      }
      frontier ++= workflow.startOperators.flatMap(workflow.outLinks(_))
    case ContinuedInitialization =>
      log.info("continue initialization")
      prevFrontier = frontier
      val nodes = availableNodes
      for (k <- frontier) {
        val v = workflow.operators(k)
        if (!principalBiMap.containsKey(k)) {
          val newPrincipal = context.actorOf(
            Principal.props(v).withDeploy(Deploy(scope = RemoteScope(getPrincipalNode(nodes)))),
            v.tag.operator
          )
          principalBiMap.put(k, newPrincipal)
        }
        val p = principalBiMap.get(k)
        principalStates(p) = PrincipalState.Uninitialized
        principalInCurrentStage.add(p)
      }
      frontier.foreach { x =>
        val principal = principalBiMap.get(x)
        AdvancedMessageSending.nonBlockingAskWithRetry(
          principal,
          AckedPrincipalInitialization(Array()),
          10,
          0,
          y =>
            y match {
              case AckWithInformation(z) =>
                workflow.operators(x) = z.asInstanceOf[OpExecConfig]
                //assign exception breakpoint before all breakpoints
                if (!recoveryMode) {
                  AdvancedMessageSending.blockingAskWithRetry(
                    principal,
                    AssignBreakpoint(
                      new ExceptionGlobalBreakpoint(x.operator + "-ExceptionBreakpoint")
                    ),
                    10
                  )
                }
              case other => throw new AmberException("principal didn't return updated metadata")
            }
        )
      }
      val next = frontier.flatMap(workflow.outLinks(_))
      frontier.clear()
      frontier ++= next
    case PrincipalMessage.ReportState(state) =>
      principalStates(sender) = state
      if (
        principalStates.size == workflow.operators.size && principalStates.values.forall(
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
        context.parent ! ReportState(ControllerState.Ready)
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
            .forall(x =>
              principalBiMap.containsKey(x) && principalStates.contains(
                principalBiMap.get(x)
              ) && principalStates(principalBiMap.get(x)) == PrincipalState.Ready
            )
        )
        frontier --= next
        val prevInfo = next
          .flatMap(
            workflow
              .inLinks(_)
              .map(x =>
                (
                  workflow.operators(x),
                  Await
                    .result(principalBiMap.get(x) ? GetOutputLayer, 5.seconds)
                    .asInstanceOf[ActorLayer]
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
          if (!principalBiMap.containsKey(k)) {
            val p = context.actorOf(
              Principal.props(v).withDeploy(Deploy(scope = RemoteScope(getPrincipalNode(nodes)))),
              v.tag.operator
            )
            principalBiMap.put(k, p)
          }
          val principal = principalBiMap.get(k)
          principalStates(principal) = PrincipalState.Uninitialized
          principalInCurrentStage.add(principal)
          AdvancedMessageSending.blockingAskWithRetry(
            principal,
            AckedPrincipalInitialization(prevInfo),
            10,
            y =>
              y match {
                case AckWithInformation(z) =>
                  workflow.operators(k) = z.asInstanceOf[OpExecConfig]
                  //assign exception breakpoint before all breakpoints
                  if (!recoveryMode) {
                    AdvancedMessageSending.blockingAskWithRetry(
                      principal,
                      AssignBreakpoint(
                        new ExceptionGlobalBreakpoint(k.operator + "-ExceptionBreakpoint")
                      ),
                      10
                    )
                  }
                case other => throw new AmberException("principal didn't return updated metadata")
              }
          )
          for (from <- workflow.inLinks(k)) {
            if (!linksToIgnore.contains(from, k)) {
              val edge = new OperatorLink(
                (workflow.operators(from), principalBiMap.get(from)),
                (workflow.operators(k), principalBiMap.get(k))
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
    case msg => stash()
  }

  private[this] def ready: Receive = {
    case QueryStatistics =>
      this.principalBiMap.values().forEach(principal => principal ! QueryStatistics)
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case Start =>
      log.info("received start signal")
      if (!timer.isRunning) {
        timer.start()
      }
      workflow.startOperators.foreach { x =>
        if (!startDependencies.contains(x))
          AdvancedMessageSending.nonBlockingAskWithRetry(principalBiMap.get(x), Start, 10, 0)
      }
    case PrincipalMessage.ReportState(state) =>
      principalStates(sender) = state
      state match {
        case PrincipalState.Running =>
          log.info("edu.uci.ics.texera.workflow started!")
          context.parent ! ReportState(ControllerState.Running)
          context.become(running)
          unstashAll()
        case PrincipalState.Paused =>
          if (principalStates.values.forall(_ == PrincipalState.Completed)) {
            timer.stop()
            log.info("edu.uci.ics.texera.workflow completed! Time Elapsed: " + timer.toString())
            timer.reset()
            safeRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Completed)
            context.become(completed)
            unstashAll()
          } else if (allUnCompletedPrincipalStates.forall(_ == PrincipalState.Paused)) {
            pauseTimer.stop()
            log.info("edu.uci.ics.texera.workflow paused! Time Elapsed: " + pauseTimer.toString())
            pauseTimer.reset()
            safeRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Paused)
            if (this.eventListener.workflowPausedListener != null) {
              this.eventListener.workflowPausedListener.apply(WorkflowPaused())
            }
            context.become(paused)
            unstashAll()
          }
      }
    case PassBreakpointTo(id: String, breakpoint: GlobalBreakpoint) =>
      val opTag = OperatorIdentifier(tag, id)
      if (principalBiMap.containsKey(opTag)) {
        AdvancedMessageSending.blockingAskWithRetry(
          principalBiMap.get(opTag),
          AssignBreakpoint(breakpoint),
          3
        )
      } else {
        throw new AmberException("target operator not found")
      }
    case msg =>
      log.info("Stashing: " + msg)
      stash()
  }

  private[this] def running: Receive = {
    case KillAndRecover =>
      killAndRecoverStage()
    case QueryStatistics =>
      this.principalBiMap.values().forEach(principal => principal ! QueryStatistics)
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case PrincipalMessage.ReportState(state) =>
      principalStates(sender) = state
      state match {
        case PrincipalState.Completed =>
          log.info(sender + " completed")
          if (stashedNodes.contains(sender)) {
            AdvancedMessageSending.nonBlockingAskWithRetry(sender, ReleaseOutput, 10, 0)
            stashedNodes.remove(sender)
          }
          if (principalStates.values.forall(_ == PrincipalState.Completed)) {
            timer.stop()
            log.info("edu.uci.ics.texera.workflow completed! Time Elapsed: " + timer.toString())
            timer.reset()
            safeRemoveAskHandle()
            if (frontier.isEmpty) {
              context.parent ! ReportState(ControllerState.Completed)
              context.become(completed)
              // collect all output results back to controller
              val sinkPrincipals =
                this.workflow.endOperators.map(sinkOp => this.principalBiMap.get(sinkOp))
              for (sinkPrincipal <- sinkPrincipals) {
                sinkPrincipal ! CollectSinkResults
              }
              if (this.statusUpdateAskHandle != null) {
                this.statusUpdateAskHandle.cancel()
                self ! QueryStatistics
              }
//              self ! PoisonPill
            } else {
              recoveryMode = false
              principalInCurrentStage.clear()
              context.become(receive)
              self ! ContinuedInitialization
            }
            unstashAll()
          }
        case PrincipalState.CollectingBreakpoints =>
        case PrincipalState.Paused =>
          if (principalStates.values.forall(_ == PrincipalState.Completed)) {
            if (timer.isRunning) {
              timer.stop()
            }
            log.info("edu.uci.ics.texera.workflow completed! Time Elapsed: " + timer.toString())
            timer.reset()
            safeRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Completed)
            context.become(completed)
            unstashAll()
          } else if (allUnCompletedPrincipalStates.forall(_ == PrincipalState.Paused)) {
            if (pauseTimer.isRunning) {
              pauseTimer.stop()
            }
            log.info("edu.uci.ics.texera.workflow paused! Time Elapsed: " + pauseTimer.toString())
            pauseTimer.reset()
            safeRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Paused)
            if (this.eventListener.workflowPausedListener != null) {
              this.eventListener.workflowPausedListener.apply(WorkflowPaused())
            }
            context.become(paused)
            unstashAll()
          }
        case _ => //skip others
      }
    case ReportGlobalBreakpointTriggered(bp, opID) =>
      self ! Pause
      context.parent ! ReportGlobalBreakpointTriggered(bp, opID)
      if (this.eventListener.breakpointTriggeredListener != null) {
        this.eventListener.breakpointTriggeredListener.apply(BreakpointTriggered(bp, opID))
      }
      log.info(bp.toString())
    case Pause =>
      pauseTimer.start()
      workflow.operators.foreach(x => principalBiMap.get(x._1) ! Pause)
      //edu.uci.ics.texera.workflow.startOperators.foreach(principalBiMap.get(_) ! Pause)
      //frontier ++= edu.uci.ics.texera.workflow.startOperators.flatMap(edu.uci.ics.texera.workflow.outLinks(_))
      log.info("received pause signal")
      safeRemoveAskHandle()
      periodicallyAskHandle =
        context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
      context.parent ! ReportState(ControllerState.Pausing)
      context.become(pausing)
      unstashAll()
    case ReportPrincipalPartialCompleted(from, layer) =>
      sender ! Ack
      for (i <- startDependencies.keys) {
        if (startDependencies(i).contains(from) && startDependencies(i)(from).contains(layer)) {
          startDependencies(i)(from) -= layer
          if (startDependencies(i)(from).isEmpty) {
            startDependencies(i) -= from
            if (startDependencies(i).isEmpty) {
              startDependencies -= i
              AdvancedMessageSending.nonBlockingAskWithRetry(principalBiMap.get(i), Start, 10, 0)
            }
          }
        }
      }
    case Resume =>
    case msg    => stash()
  }

  private[this] def pausing: Receive = {
    case QueryStatistics =>
      this.principalBiMap.values().forEach(principal => principal ! QueryStatistics)
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case EnforceStateCheck =>
      frontier.flatMap(workflow.inLinks(_)).foreach(principalBiMap.get(_) ! QueryState)
    case reportCurrentProcessingTuple: ReportCurrentProcessingTuple =>
      if (this.eventListener.reportCurrentTuplesListener != null) {
        this.eventListener.reportCurrentTuplesListener.apply(reportCurrentProcessingTuple);
      }
    case PrincipalMessage.ReportState(state) =>
      if (
        state != PrincipalState.Paused && state != PrincipalState.Pausing && state != PrincipalState.Completed
      ) {
        sender ! Pause
      } else {
        principalStates(sender) = state
        if (principalStates.values.forall(_ == PrincipalState.Completed)) {
          timer.stop()
          frontier.clear()
          log.info("edu.uci.ics.texera.workflow completed! Time Elapsed: " + timer.toString())
          timer.reset()
          safeRemoveAskHandle()
          if (frontier.isEmpty) {
            context.parent ! ReportState(ControllerState.Completed)
            context.become(completed)
          } else {
            context.become(receive)
            self ! ContinuedInitialization
          }
          unstashAll()
        } else if (allUnCompletedPrincipalStates.forall(_ == PrincipalState.Paused)) {
          if (pauseTimer.isRunning) {
            pauseTimer.stop()
          }
          frontier.clear()
          log.info("edu.uci.ics.texera.workflow paused! Time Elapsed: " + pauseTimer.toString())
          pauseTimer.reset()
          safeRemoveAskHandle()
          context.parent ! ReportState(ControllerState.Paused)
          if (this.eventListener.workflowPausedListener != null) {
            this.eventListener.workflowPausedListener.apply(WorkflowPaused())
          }
          context.become(paused)
          unstashAll()
        } else {
          val next = frontier.filter(i =>
            workflow
              .inLinks(i)
              .map(x => principalStates(principalBiMap.get(x)))
              .forall(x => x == PrincipalState.Paused || x == PrincipalState.Completed)
          )
          frontier --= next
          next.foreach(principalBiMap.get(_) ! Pause)
          frontier ++= next.filter(workflow.outLinks.contains).flatMap(workflow.outLinks(_))
        }
      }
    case ReportGlobalBreakpointTriggered(bp, opID) =>
      context.parent ! ReportGlobalBreakpointTriggered(bp, opID)
      if (this.eventListener.breakpointTriggeredListener != null) {
        this.eventListener.breakpointTriggeredListener.apply(BreakpointTriggered(bp, opID))
      }
    case msg => stash()
  }

  private[this] def paused: Receive = {
    case KillAndRecover =>
      killAndRecoverStage()
    case QueryStatistics =>
      this.principalBiMap.values().forEach(principal => principal ! QueryStatistics)
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case Resume =>
      workflow.endOperators.foreach(principalBiMap.get(_) ! Resume)
      frontier ++= workflow.endOperators.flatMap(workflow.inLinks(_))
      log.info("received resume signal")
      safeRemoveAskHandle()
      periodicallyAskHandle =
        context.system.scheduler.schedule(30.seconds, 30.seconds, self, EnforceStateCheck)
      context.parent ! ReportState(ControllerState.Resuming)
      context.become(resuming)
      unstashAll()
    case Pause                    =>
    case EnforceStateCheck        =>
    case ModifyLogic(newMetadata) =>
      // newLogic is now an OperatorMetadata
      val principal: ActorRef = principalBiMap.get(newMetadata.tag)
      log.info("modify logic received by controller, sending to principal")
      AdvancedMessageSending.blockingAskWithRetry(principal, ModifyLogic(newMetadata), 3)
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
      if (principalBiMap.containsKey(opTag)) {
        AdvancedMessageSending.blockingAskWithRetry(
          principalBiMap.get(opTag),
          AssignBreakpoint(breakpoint),
          3
        )
      } else {
        throw new AmberException("target operator not found")
      }
    case msg => stash()
  }

  private[this] def resuming: Receive = {
    case QueryStatistics =>
      this.principalBiMap.values().forEach(principal => principal ! QueryStatistics)
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case EnforceStateCheck =>
      frontier.flatMap(workflow.outLinks(_)).foreach(principalBiMap.get(_) ! QueryState)
    case PrincipalMessage.ReportState(state) =>
      if (
        state != PrincipalState.Resuming && state != PrincipalState.Running && state != PrincipalState.Ready
      ) {
        sender ! Resume
      } else {
        principalStates(sender) = state
        if (principalStates.values.forall(_ != PrincipalState.Paused)) {
          frontier.clear()
          if (principalStates.values.exists(_ != PrincipalState.Ready)) {
            log.info("edu.uci.ics.texera.workflow resumed!")
            safeRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Running)
            context.become(running)
            unstashAll()
          } else {
            log.info("edu.uci.ics.texera.workflow ready!")
            safeRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Ready)
            context.become(ready)
            unstashAll()
          }
        } else {
          val next = frontier.filter(i =>
            !workflow
              .outLinks(i)
              .map(x => principalStates(principalBiMap.get(x)))
              .contains(PrincipalState.Paused)
          )
          frontier --= next
          next.foreach(principalBiMap.get(_) ! Resume)
          frontier ++= next.filter(workflow.inLinks.contains).flatMap(workflow.inLinks(_))
        }
      }
    case msg => stash()
  }

  private[this] def completed: Receive = {
    case QueryStatistics =>
      this.principalBiMap.values().forEach(principal => principal ! QueryStatistics)
      this.exitIfCompleted
    case PrincipalMessage.ReportStatistics(statistics) =>
      principalStatisticsMap.update(sender, statistics)
      triggerStatusUpdateEvent();
    case PrincipalMessage.ReportOutputResult(sinkResults) =>
      val operatorID = this.principalBiMap.inverse().get(sender()).operator
      this.principalSinkResultMap(operatorID) = sinkResults
      if (this.principalSinkResultMap.size == this.workflow.endOperators.size) {
        if (this.eventListener.workflowCompletedListener != null) {
          this.eventListener.workflowCompletedListener
            .apply(WorkflowCompleted(this.principalSinkResultMap.toMap))
        }
      }
      this.exitIfCompleted
    case msg =>
      log.info("received: {} after edu.uci.ics.texera.workflow completed!", msg)
      if (sender != self && !principalStates.keySet.contains(sender)) {
        sender ! ReportState(ControllerState.Completed)
      }
      this.exitIfCompleted
  }

  private[this] def exitIfCompleted: Unit = {
    val reportStatistics = this.eventListener.workflowStatusUpdateListener != null;
    val reportOutputResult = this.eventListener.workflowCompletedListener != null;

    val reportStatisticsCompleted = !reportStatistics ||
      this.principalStatisticsMap.values.forall(v => v.operatorState == PrincipalState.Completed)
    val reportOutputResultCompleted = !reportOutputResult ||
      this.principalSinkResultMap.size == this.workflow.endOperators.size

    if (reportStatisticsCompleted && reportOutputResultCompleted) {
      self ! PoisonPill;
    }

  }

}
