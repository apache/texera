package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.enums.ClusterStatus
import edu.uci.ics.texera.web.model.jooq.generated.tables.Cluster.CLUSTER
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.ClusterDao
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterResource.{
  ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE,
  clusterDao,
  context
}
import io.dropwizard.auth.Auth
import org.glassfish.jersey.media.multipart.FormDataParam
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Cluster
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterServiceClient.{
  callCreateClusterAPI,
  callDeleteClusterAPI
}
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterUtils.{
  updateClusterActivityEndTime,
  updateClusterStatus
}

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs.{Consumes, ForbiddenException, GET, POST, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}

object ClusterResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val clusterDao = new ClusterDao(context.configuration)

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
  ): Response = {
    val cluster = new Cluster()
    cluster.setName(name)
    cluster.setOwnerId(user.getUid)
    cluster.setMachineType(machineType)
    cluster.setNumberOfMachines(numberOfMachines)
    cluster.setStatus(ClusterStatus.LAUNCHING)
    clusterDao.insert(cluster)

    // Call Go microservice to actually create the cluster
    callCreateClusterAPI(cluster.getCid, machineType, numberOfMachines) match {
      case Right(goResponse) =>
        Response.ok(clusterDao.fetchOneByCid(cluster.getCid)).build()

      case Left(errorMessage) =>
        updateClusterStatus(cluster.getCid, ClusterStatus.FAILED, context)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"Cluster creation failed: $errorMessage")
          .build()
    }
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
    val clusterId = cluster.getCid
    validateClusterOwnership(user, clusterId)

    updateClusterStatus(clusterId, ClusterStatus.TERMINATING, context)

    // Call Go microservice to actually delete the cluster
    callDeleteClusterAPI(clusterId) match {
      case Right(goResponse) =>
        Response.ok(goResponse).build()

      case Left(errorMessage) =>
        updateClusterStatus(
          clusterId,
          ClusterStatus.FAILED,
          context
        ) // Assuming you have a FAILED status
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"Cluster deletion failed: $errorMessage")
          .build()
    }
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
    validateClusterOwnership(user, cluster.getCid)

    updateClusterStatus(cluster.getCid, ClusterStatus.PAUSING, context)

    // TODO: Call the Go Microservice
    // TODO: need to consider if the pause fails

    updateClusterStatus(cluster.getCid, ClusterStatus.PAUSED, context)

    updateClusterActivityEndTime(cluster.getCid, context)

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
    validateClusterOwnership(user, cluster.getCid)

    updateClusterStatus(cluster.getCid, ClusterStatus.RESUMING, context)

    // TODO: Call the Go Microservice
    // TODO: need to consider if the resume fails

    updateClusterStatus(cluster.getCid, ClusterStatus.LAUNCHED, context)

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
    val clusterOwnerId = clusterDao.fetchOneByCid(clusterId).getOwnerId
    if (clusterOwnerId != user.getUid) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE)
    }
  }
}
