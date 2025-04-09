package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.enums.ClusterStatus
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.ClusterDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.Cluster
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterResource.{
  ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE,
  clusterDao,
  context
}
import io.dropwizard.auth.Auth
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterServiceClient.{
  callCreateClusterAPI,
  callDeleteClusterAPI,
  callPauseClusterAPI,
  callResumeClusterAPI
}
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterUtils.updateClusterStatus
import edu.uci.ics.texera.dao.jooq.generated.tables.Cluster.CLUSTER

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs.{Consumes, ForbiddenException, GET, POST, Path, QueryParam}
import javax.ws.rs.core.{MediaType, Response}

object ClusterResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val clusterDao = new ClusterDao(context.configuration)

  // error messages
  val ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE = "User has no access to this cluster"
}

@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/cluster")
class ClusterResource {

  /**
    * Launchs a new cluster and records the start time in cluster_activity.
    *
    * @param user The authenticated user creating the cluster.
    * @param launchRequest The launch request containing cluster configuration.
    * @return The created Cluster object.
    */
  @POST
  @Path("/launch")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def launchCluster(
      @Auth user: SessionUser,
      launchRequest: ClusterLaunchRequest
  ): Response = {
    val cluster = new Cluster()
    cluster.setName(launchRequest.name)
    cluster.setOwnerId(user.getUid)
    cluster.setMachineType(launchRequest.machineType)
    cluster.setNumberOfMachines(launchRequest.numberOfMachines)
    cluster.setStatus(ClusterStatus.LAUNCH_RECEIVED)
    clusterDao.insert(cluster)

    // Call Go microservice to actually create the cluster
    callCreateClusterAPI(
      cluster.getCid,
      launchRequest.machineType,
      launchRequest.numberOfMachines
    ) match {
      case Right(goResponse) =>
        updateClusterStatus(cluster.getCid, ClusterStatus.PENDING, context)
        Response.ok(clusterDao.fetchOneByCid(cluster.getCid)).build()

      case Left(errorMessage) =>
        updateClusterStatus(cluster.getCid, ClusterStatus.LAUNCH_FAILED, context)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"Cluster creation failed: $errorMessage")
          .build()
    }
  }

  /**
    * Terminates a cluster and records the termination time in cluster_activity.
    *
    * @param user The authenticated user requesting the deletion.
    * @param cluster The cluster to be deleted.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Path("/terminate")
  def terminateCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    val clusterId = cluster.getCid
    validateClusterOwnership(user, clusterId)

    updateClusterStatus(clusterId, ClusterStatus.TERMINATE_RECEIVED, context)

    // Call Go microservice to actually delete the cluster
    callDeleteClusterAPI(clusterId) match {
      case Right(goResponse) =>
        updateClusterStatus(clusterId, ClusterStatus.SHUTTING_DOWN, context)
        Response.ok(goResponse).build()

      case Left(errorMessage) =>
        updateClusterStatus(
          clusterId,
          ClusterStatus.TERMINATE_FAILED,
          context
        )
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"Cluster deletion failed: $errorMessage")
          .build()
    }
  }

  /**
    * Stops a cluster and records the stop time in cluster_activity.
    *
    * @param user The authenticated user requesting the pause.
    * @param cluster The cluster to be paused.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Path("/stop")
  def stopCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    val clusterId = cluster.getCid
    validateClusterOwnership(user, clusterId)

    updateClusterStatus(clusterId, ClusterStatus.STOP_RECEIVED, context)

    callPauseClusterAPI(clusterId) match {
      case Right(goResponse) =>
        updateClusterStatus(clusterId, ClusterStatus.STOPPING, context)
        Response.ok(goResponse).build()

      case Left(errorMessage) =>
        updateClusterStatus(
          clusterId,
          ClusterStatus.STOP_FAILED,
          context
        )
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"Cluster pause failed: $errorMessage")
          .build()
    }

  }

  /**
    * Starts a stopped cluster and records the start time in cluster_activity.
    *
    * @param user The authenticated user requesting the resume.
    * @param cluster The cluster to be resumed.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Path("/start")
  def startCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    val clusterId = cluster.getCid
    validateClusterOwnership(user, clusterId)

    updateClusterStatus(clusterId, ClusterStatus.START_RECEIVED, context)

    callResumeClusterAPI(clusterId) match {
      case Right(goResponse) =>
        updateClusterStatus(clusterId, ClusterStatus.PENDING, context)
        Response.ok(goResponse).build()

      case Left(errorMessage) =>
        updateClusterStatus(
          clusterId,
          ClusterStatus.START_FAILED,
          context
        )
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"Cluster resume failed: $errorMessage")
          .build()
    }

  }

  /**
    * Updates the name of a cluster.
    *
    * @param user The authenticated user requesting the update.
    * @param cluster The cluster with the new name.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/update/name")
  def updateClusterName(@Auth user: SessionUser, cluster: Cluster): Response = {
    validateClusterOwnership(user, cluster.getCid)

    context
      .update(CLUSTER)
      .set(CLUSTER.NAME, cluster.getName)
      .where(CLUSTER.CID.eq(cluster.getCid))
      .execute()

    Response.ok().build()
  }

  /**
    * Lists all clusters owned by the authenticated user.
    *
    * @param user The authenticated user.
    * @param available Boolean to indicate whether to return available (Launched) clusters only
    * @return A list of Clusters owned by the user.
    */
  @GET
  @Path("")
  def listClusters(
      @Auth user: SessionUser,
      @QueryParam("available") available: Boolean
  ): util.List[Cluster] = {
    clusterDao.fetchByOwnerId(user.getUid)
    var steps = context
      .select(CLUSTER.asterisk())
      .from(CLUSTER)
      .where(CLUSTER.OWNER_ID.eq(user.getUid))
      .and(CLUSTER.STATUS.ne(ClusterStatus.TERMINATED))
    if (available) {
      steps = steps.and(CLUSTER.STATUS.eq(ClusterStatus.RUNNING))
    }
    steps.fetchInto(classOf[Cluster])

  }

  /**
    * Validates that the authenticated user has ownership of the cluster.
    *
    * @param user The authenticated user.
    * @param clusterId The ID of the cluster to validate ownership.
    */
  private def validateClusterOwnership(user: SessionUser, clusterId: Int): Unit = {
    val clusterOwnerId = clusterDao.fetchOneByCid(clusterId).getOwnerId
    if (clusterOwnerId != user.getUid) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE)
    }
  }
}

/**
  * Request object for launching a cluster.
  */
case class ClusterLaunchRequest(
    name: String,
    machineType: String,
    numberOfMachines: Integer
)
