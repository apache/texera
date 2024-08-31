package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.enums.ClusterStatus

import javax.ws.rs.{Consumes, POST, Path}
import javax.ws.rs.core.{MediaType, Response}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{ClusterActivityDao, ClusterDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.ClusterActivity
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterUtils.{
  updateClusterActivityEndTime,
  updateClusterStatus
}
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterCallbackResource.{
  clusterActivityDao,
  clusterDao,
  context
}

import java.sql.Timestamp

object ClusterCallbackResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val clusterDao = new ClusterDao(context.configuration)
  final private lazy val clusterActivityDao = new ClusterActivityDao(context.configuration)

  // error messages
  val ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE = "User has no access to this cluster"
}

@Path("/callback")
class ClusterCallbackResource {

  @POST
  @Path("/cluster/created")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def handleClusterCreatedCallback(callbackPayload: CallbackPayload): Response = {
    val clusterId = callbackPayload.clusterId
    val success = callbackPayload.success;
    // Update the cluster status to LAUNCHED in the database
    val cluster = clusterDao.fetchOneByCid(clusterId)
    if (success && cluster != null && cluster.getStatus == ClusterStatus.LAUNCHING) {
      updateClusterStatus(clusterId, ClusterStatus.LAUNCHED, context)
      insertClusterActivity(cluster.getCid, cluster.getCreationTime)
      Response.ok("Cluster status updated to LAUNCHED").build()
    } else {
      Response
        .status(Response.Status.NOT_FOUND)
        .entity("Cluster not found or status update not allowed")
        .build()
    }
  }

  @POST
  @Path("/cluster/deleted")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def handleClusterDeletedCallback(callbackPayload: CallbackPayload): Response = {
    val clusterId = callbackPayload.clusterId
    val success = callbackPayload.success;

    val cluster = clusterDao.fetchOneByCid(clusterId)
    if (success && cluster != null && cluster.getStatus == ClusterStatus.TERMINATING) {
      updateClusterStatus(clusterId, ClusterStatus.TERMINATED, context)
      updateClusterActivityEndTime(clusterId, context)
      Response
        .ok(s"Cluster with ID $clusterId marked as TERMINATED and activity end time updated")
        .build()
    } else {
      Response
        .status(Response.Status.NOT_FOUND)
        .entity("Cluster not found or status update not allowed")
        .build()
    }
  }

  /**
    * Inserts a new cluster activity record with the given start time.
    *
    * @param clusterId The ID of the cluster.
    * @param startTime The start time of the activity.
    */
  private def insertClusterActivity(clusterId: Int, startTime: Timestamp): Unit = {
    val clusterActivity = new ClusterActivity()
    clusterActivity.setClusterId(clusterId)
    clusterActivity.setStartTime(startTime)
    clusterActivityDao.insert(clusterActivity)
  }
}

// Define the payload structure expected from the Go service
case class CallbackPayload(clusterId: Int, success: Boolean)
