package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.enums.ClusterStatus
import edu.uci.ics.texera.web.model.jooq.generated.tables.Cluster.CLUSTER
import edu.uci.ics.texera.web.model.jooq.generated.tables.ClusterActivity.CLUSTER_ACTIVITY
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{ClusterActivityDao, ClusterDao}
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterResource.{
  ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE,
  clusterActivityDao,
  clusterDao,
  context
}
import io.dropwizard.auth.Auth
import org.glassfish.jersey.media.multipart.FormDataParam
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Cluster, ClusterActivity}
import org.jooq.impl.DSL.max

import java.sql.Timestamp
import java.time.Instant
import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs.{Consumes, ForbiddenException, GET, POST, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}

object ClusterResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val clusterDao = new ClusterDao(context.configuration)
  final private lazy val clusterActivityDao = new ClusterActivityDao(context.configuration)

  // error messages
  val ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE = "User has no access to this cluster"
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/cluster")
class ClusterResource {

  /**
    * Creates a new cluster and records the start time in cluster_activity.
    *
    * @param user The authenticated user creating the cluster.
    * @param name The name of the cluster.
    * @param machineType The type of machines in the cluster.
    * @param numberOfMachines The number of machines in the cluster.
    * @return The created Cluster object.
    */
  @POST
  @Path("/create")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def createCluster(
      @Auth user: SessionUser,
      @FormDataParam("Name") name: String,
      @FormDataParam("machineType") machineType: String,
      @FormDataParam("numberOfMachines") numberOfMachines: Integer
  ): Cluster = {
    val cluster = new Cluster()
    cluster.setName(name)
    cluster.setOwnerId(user.getUid)
    cluster.setMachineType(machineType)
    cluster.setNumberOfMachines(numberOfMachines)
    cluster.setStatus(ClusterStatus.LAUNCHING)
    clusterDao.insert(cluster)

    // TODO: use Go microservice to launch the cluster
    // TODO: need to consider if the creation fails

    cluster.setStatus(ClusterStatus.LAUNCHED)
    clusterDao.update(cluster)

    insertClusterActivity(cluster.getId, cluster.getCreationTime)

    clusterDao.fetchOneById(cluster.getId)
  }

  /**
    * Deletes a cluster and records the termination time in cluster_activity.
    *
    * @param user The authenticated user requesting the deletion.
    * @param cluster The cluster to be deleted.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Path("/delete")
  def deleteCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    validateClusterOwnership(user, cluster.getId)

    updateClusterStatus(cluster.getId, ClusterStatus.TERMINATING)

    // TODO: Call the Go Microservice
    // TODO: need to consider if the deletion fails

    updateClusterStatus(cluster.getId, ClusterStatus.TERMINATED)

    updateClusterActivityEndTime(cluster.getId)

    Response.ok().build()
  }

  /**
    * Pauses a cluster and records the pause time in cluster_activity.
    *
    * @param user The authenticated user requesting the pause.
    * @param cluster The cluster to be paused.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Path("/pause")
  def pauseCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    validateClusterOwnership(user, cluster.getId)

    updateClusterStatus(cluster.getId, ClusterStatus.PAUSING)

    // TODO: Call the Go Microservice
    // TODO: need to consider if the pause fails

    updateClusterStatus(cluster.getId, ClusterStatus.PAUSED)

    updateClusterActivityEndTime(cluster.getId)

    Response.ok().build()
  }

  /**
    * Resumes a paused cluster and records the resume time in cluster_activity.
    *
    * @param user The authenticated user requesting the resume.
    * @param cluster The cluster to be resumed.
    * @return A Response indicating the result of the operation.
    */
  @POST
  @Path("/resume")
  def resumeCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    validateClusterOwnership(user, cluster.getId)

    updateClusterStatus(cluster.getId, ClusterStatus.RESUMING)

    // TODO: Call the Go Microservice
    // TODO: need to consider if the resume fails

    updateClusterStatus(cluster.getId, ClusterStatus.LAUNCHED)

    insertClusterActivity(cluster.getId, Timestamp.from(Instant.now()))

    Response.ok().build()
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
    validateClusterOwnership(user, cluster.getId)

    context
      .update(CLUSTER)
      .set(CLUSTER.NAME, cluster.getName)
      .where(CLUSTER.ID.eq(cluster.getId))
      .execute()

    Response.ok().build()
  }

  /**
    * Lists all clusters owned by the authenticated user.
    *
    * @param user The authenticated user.
    * @return A list of Clusters owned by the user.
    */
  @GET
  @Path("")
  def listClusters(@Auth user: SessionUser): util.List[Cluster] = {
    clusterDao.fetchByOwnerId(user.getUid)
  }

  /**
    * Validates that the authenticated user has ownership of the cluster.
    *
    * @param user The authenticated user.
    * @param clusterId The ID of the cluster to validate ownership.
    */
  private def validateClusterOwnership(user: SessionUser, clusterId: Int): Unit = {
    val clusterOwnerId = clusterDao.fetchOneById(clusterId).getOwnerId
    if (clusterOwnerId != user.getUid) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE)
    }
  }

  /**
    * Updates the status of a cluster.
    *
    * @param clusterId The ID of the cluster to update.
    * @param status The new status of the cluster.
    */
  private def updateClusterStatus(clusterId: Int, status: ClusterStatus): Unit = {
    context
      .update(CLUSTER)
      .set(CLUSTER.STATUS, status)
      .where(CLUSTER.ID.eq(clusterId))
      .execute()
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

  /**
    * Updates the end time of the most recent cluster activity to the current time.
    *
    * @param clusterId The ID of the cluster.
    */
  private def updateClusterActivityEndTime(clusterId: Int): Unit = {
    context
      .update(CLUSTER_ACTIVITY)
      .set(CLUSTER_ACTIVITY.END_TIME, Timestamp.from(Instant.now()))
      .where(CLUSTER_ACTIVITY.CLUSTER_ID.eq(clusterId))
      .and(
        CLUSTER_ACTIVITY.START_TIME.eq(
          context
            .select(max(CLUSTER_ACTIVITY.START_TIME))
            .from(CLUSTER_ACTIVITY)
            .where(CLUSTER_ACTIVITY.CLUSTER_ID.eq(clusterId))
            .and(CLUSTER_ACTIVITY.END_TIME.isNull)
        )
      )
      .execute()
  }
}
