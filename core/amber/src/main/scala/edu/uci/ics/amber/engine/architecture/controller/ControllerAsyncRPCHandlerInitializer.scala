package edu.uci.ics.amber.engine.architecture.controller

import com.twitter.util.Future
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelMarkerIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.core.workflow.PhysicalPlan
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers._
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerType.{NO_ALIGNMENT, REQUIRE_ALIGNMENT}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_NO_OPERATION
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.texera.web.service.FriesReconfigurationAlgorithm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ControllerAsyncRPCHandlerInitializer(
    val cp: ControllerProcessor
) extends AsyncRPCHandlerInitializer(cp.asyncRPCClient, cp.asyncRPCServer)
    with ControllerServiceFs2Grpc[Future, AsyncRPCContext]
    with AmberLogging
    with LinkWorkersHandler
    with WorkerExecutionCompletedHandler
    with WorkerStateUpdatedHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with PortCompletedHandler
    with ConsoleMessageHandler
    with RetryWorkflowHandler
    with EvaluatePythonExpressionHandler
    with DebugCommandHandler
    with TakeGlobalCheckpointHandler
    with ChannelMarkerHandler
    with RetrieveWorkflowStateHandler
    with BroadcastMessageHandler{
  val actorId: ActorVirtualIdentity = cp.actorId


  def getCurrentTimeString: String = {
    val now = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    now.format(formatter)
  }

  override def retrieveOpStateReverseTopo(request: RetrieveOpStateReverseTopoRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val dag = cp.workflowScheduler.physicalPlan.dag // jGraphT DAG
    val targets = request.targets.toSet
    val visited = mutable.Set[PhysicalOpIdentity]()
    val levelMap = mutable.Map[PhysicalOpIdentity, Int]()
    val levels = mutable.ListBuffer[mutable.Set[PhysicalOpIdentity]]()

    // Assign levels using DFS
    def assignLevels(node: PhysicalOpIdentity): Int = {
      if (levelMap.contains(node)) return levelMap(node)

      val parents = dag.incomingEdgesOf(node).asScala.map(dag.getEdgeSource)
      val parentLevel = parents.map(assignLevels).maxOption.getOrElse(-1)
      val currentLevel = parentLevel + 1
      levelMap(node) = currentLevel

      while (levels.size <= currentLevel) {
        levels += mutable.Set()
      }
      levels(currentLevel) += node

      currentLevel
    }

    // Assign levels starting from targets
    targets.foreach(assignLevels)

    // Traverse levels from high to low (reverse topological order)
    levels.reverse.foldLeft(Future.Done) { (acc, levelNodes) =>
      acc.flatMap { _ =>
        val levelFutures = levelNodes.toSeq.flatMap { node =>
          if (!visited.contains(node)) {
            visited += node
            val workers = cp.workflowExecution.getLatestOperatorExecution(node).getWorkerIds
            workers.map { worker =>
              val context = mkContext(worker)
              workerInterface.noOperation(EmptyRequest(), context)
            }
          } else Seq.empty
        }
        Future.collect(levelFutures)
        ()
      }
    }.map(_ => EmptyReturn())

  }

  override def retrieveOpStateViaMarker(request: RetrieveOpStateViaMarkerRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val region = cp.workflowExecutionCoordinator.getExecutingRegions.head
    val mcs = FriesReconfigurationAlgorithm.computeMCS(region, request.targets.toSet)
    val newLinks =
      region.getLinks.filter(link =>
        mcs.contains(link.fromOpId) && mcs.contains(link.toOpId)
      )
    val mcsPlan = PhysicalPlan(mcs.map(opId => region.getOperator(opId)), newLinks)
    controllerInterface.propagateChannelMarker(
      PropagateChannelMarkerRequest(
        mcsPlan.getSourceOperatorIds.toSeq,
        ChannelMarkerIdentity(s"retrieveOpStateViaMarker-${getCurrentTimeString}"),
        NO_ALIGNMENT,
        mcs.toSeq,
        request.targets.toSeq,
        EmptyRequest(),
        METHOD_NO_OPERATION.getBareMethodName
      ),
      mkContext(SELF)
    ).map{
      resp =>
        EmptyReturn()
    }
  }

  override def retrieveExecStateCL(request: RetrieveExecStateCLRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val physicalOpIdsToTakeCheckpoint = request.targets
    controllerInterface
      .propagateChannelMarker(
        PropagateChannelMarkerRequest(
          cp.workflowExecution.getAllRegionExecutions
            .flatMap(_.getAllOperatorExecutions.map(_._1))
            .toSeq,
          ChannelMarkerIdentity(s"retrieveExecStateCL-$getCurrentTimeString"),
          REQUIRE_ALIGNMENT,
          cp.workflowScheduler.physicalPlan.operators.map(_.id).toSeq,
          physicalOpIdsToTakeCheckpoint.toSeq,
          EmptyRequest(),
          METHOD_NO_OPERATION.getBareMethodName
        ),
        mkContext(SELF)
      ).map(resp => EmptyReturn())
  }

  override def retrieveExecStateFlink(request: RetrieveExecStateFlinkRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val physicalOpIdsToTakeCheckpoint = request.targets
    controllerInterface
      .propagateChannelMarker(
        PropagateChannelMarkerRequest(
          cp.workflowScheduler.physicalPlan.getSourceOperatorIds.toSeq,
          ChannelMarkerIdentity(s"retrieveExecStateFlink-$getCurrentTimeString"),
          REQUIRE_ALIGNMENT,
          cp.workflowScheduler.physicalPlan.operators.map(_.id).toSeq,
          physicalOpIdsToTakeCheckpoint.toSeq,
          EmptyRequest(),
          METHOD_NO_OPERATION.getBareMethodName
        ),
        mkContext(SELF)
      ).map(resp => EmptyReturn())
  }

  override def retrieveExecStateFlinkAsync(request: RetrieveExecStateFlinkAsyncRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val physicalOpIdsToTakeCheckpoint = request.targets
    controllerInterface
      .propagateChannelMarker(
        PropagateChannelMarkerRequest(
          cp.workflowScheduler.physicalPlan.getSourceOperatorIds.toSeq,
          ChannelMarkerIdentity(s"retrieveExecStateFlinkAsync-$getCurrentTimeString"),
          NO_ALIGNMENT,
          cp.workflowScheduler.physicalPlan.operators.map(_.id).toSeq,
          physicalOpIdsToTakeCheckpoint.toSeq,
          EmptyRequest(),
          METHOD_NO_OPERATION.getBareMethodName
        ),
        mkContext(SELF)
      ).map(resp => EmptyReturn())
  }

  override def retrieveExecStatePause(request: RetrieveExecStatePauseRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val pauses = request.targets.flatMap{
      op =>
        cp.workflowExecution.getLatestOperatorExecution(op).getWorkerIds.map{
          worker =>
            workerInterface.pauseWorker(EmptyRequest(), mkContext(worker))
        }
    }
    Future.collect(pauses).flatMap{
      resp =>
        val chkpts = request.targets.flatMap{
          op =>
            cp.workflowExecution.getLatestOperatorExecution(op).getWorkerIds.map{
              worker =>
                workerInterface.noOperation(EmptyRequest(), mkContext(worker))
            }
        }
        Future.collect(chkpts).map{
          resp =>
            logger.info("retrieveExecStatePause completed")
            EmptyReturn()
        }
    }
  }
}
