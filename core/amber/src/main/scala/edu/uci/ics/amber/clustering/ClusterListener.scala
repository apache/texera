package edu.uci.ics.amber.clustering

import akka.actor.{Actor, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Future}
import edu.uci.ics.amber.clustering.ClusterListener.numWorkerNodesInCluster
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED
}
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberLogging}
import edu.uci.ics.amber.error.ErrorUtils.getStackTraceWithAllCauses
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.amber.core.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.SessionState
import edu.uci.ics.texera.web.model.websocket.response.ClusterStatusUpdateEvent
import edu.uci.ics.texera.web.service.{WorkflowExecutionService, WorkflowService}
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

object ClusterListener {
  final case class GetAvailableNodeAddresses()

  var numWorkerNodesInCluster = 0
}

class ClusterListener extends Actor with AmberLogging {

  val actorId: ActorVirtualIdentity = ActorVirtualIdentity("ClusterListener")
  val cluster: Cluster = Cluster(context.system)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent]
    )
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case evt: MemberEvent =>
      logger.info(s"received member event = $evt")
      updateClusterStatus(evt)
    case ClusterListener.GetAvailableNodeAddresses() =>
      sender() ! getAllAddress.toArray
    case other =>
      println(other)
  }

  private def getAllAddress: Iterable[Address] = {
    cluster.state.members
      .map(_.address)
  }

  private def forcefullyStop(executionService: WorkflowExecutionService, cause: Throwable): Unit = {
    executionService.client.shutdown()
    executionService.executionStateStore.statsStore.updateState(stats =>
      stats.withEndTimeStamp(System.currentTimeMillis())
    )
    executionService.executionStateStore.metadataStore.updateState { metadataStore =>
      logger.error("forcefully stopping execution", cause)
      updateWorkflowState(FAILED, metadataStore).addFatalErrors(
        WorkflowFatalError(
          EXECUTION_FAILURE,
          Timestamp(Instant.now),
          cause.toString,
          getStackTraceWithAllCauses(cause),
          "unknown operator"
        )
      )
    }
  }

  private def updateClusterStatus(evt: MemberEvent): Unit = {
    evt match {
      case MemberRemoved(member, status) =>
        logger.info("Cluster node " + member + " is down!")
        val futures = new ArrayBuffer[Future[_]]
        WorkflowService.getAllWorkflowServices.foreach { workflow =>
          val executionService = workflow.executionService.getValue
          if (
            executionService != null && executionService.executionStateStore.metadataStore.getState.state != COMPLETED
          ) {
            if (AmberConfig.isFaultToleranceEnabled) {
              logger.info(
                s"Trigger recovery process for execution id = ${executionService.executionStateStore.metadataStore.getState.executionId.id}"
              )
              try {
                futures.append(executionService.client.notifyNodeFailure(member.address))
              } catch {
                case t: Throwable =>
                  logger.warn(
                    s"execution ${executionService.workflowContext.executionId.id} cannot recover! forcing it to stop"
                  )
                  forcefullyStop(executionService, t)
              }
            } else {
              logger.info(
                s"Kill execution id = ${executionService.executionStateStore.metadataStore.getState.executionId.id}"
              )
              forcefullyStop(
                executionService,
                new RuntimeException("fault tolerance is not enabled")
              )
            }
          }
        }
        Await.all(futures.toSeq: _*)
      case other => //skip
    }

    numWorkerNodesInCluster = getAllAddress.size
    SessionState.getAllSessionStates.foreach { state =>
      state.send(ClusterStatusUpdateEvent(numWorkerNodesInCluster))
    }

    logger.info(
      "---------Now we have " + numWorkerNodesInCluster + s" nodes in the cluster---------"
    )

  }

}
