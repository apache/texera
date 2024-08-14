package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.ClusterDao
import edu.uci.ics.texera.web.resource.dashboard.user.cluster.ClusterResource.{
  ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE,
  clusterDao
}
import io.dropwizard.auth.Auth
import org.glassfish.jersey.media.multipart.FormDataParam
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Cluster

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

  @POST
  @Path("/create")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def createCluster(
      @Auth user: SessionUser,
      @FormDataParam("Name") name: String,
      @FormDataParam("machineType") machineType: String,
      @FormDataParam("numberOfMachines") numberOfMachines: Integer
  ): Cluster = {
    // TODO: Call the Go Microservice

    val cluster = new Cluster()
    cluster.setName(name)
    cluster.setOwnerId(user.getUid)
    cluster.setMachineType(machineType)
    cluster.setNumberOfMachines(numberOfMachines)

    clusterDao.insert(cluster)
    clusterDao.fetchOneById(cluster.getId)
  }

  @POST
  @Path("/delete")
  def deleteCluster(@Auth user: SessionUser, cluster: Cluster): Response = {
    val clusterId = cluster.getId
    val clusterOwnerId = clusterDao.fetchOneById(clusterId).getOwnerId
    if (clusterOwnerId != user.getUid) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE)
    }

    // TODO: Call the Go Microservice
    clusterDao.deleteById(clusterId)
    Response.ok().build()
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/update/name")
  def updateClusterName(@Auth user: SessionUser, cluster: Cluster): Response = {
    val clusterId = cluster.getId
    val newCluster = clusterDao.fetchOneById(clusterId)
    if (newCluster.getOwnerId != user.getUid) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_CLUSTER_MESSAGE)
    }

    newCluster.setName(cluster.getName)
    clusterDao.update(newCluster)
    Response.ok().build()
  }

  /**
    * This method returns a list of DashboardDatasets objects that are accessible by current user.
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @Path("")
  def listClusters(@Auth user: SessionUser): util.List[Cluster] = {
    clusterDao.fetchByOwnerId(user.getUid)
  }
}
