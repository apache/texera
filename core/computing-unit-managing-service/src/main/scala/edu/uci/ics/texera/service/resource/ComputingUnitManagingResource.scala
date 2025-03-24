package edu.uci.ics.texera.service.resource

import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowComputingUnitDao
import edu.uci.ics.texera.dao.jooq.generated.tables.WorkflowComputingUnit.WORKFLOW_COMPUTING_UNIT
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import edu.uci.ics.texera.service.resource.ComputingUnitManagingResource.{DashboardWorkflowComputingUnit, TerminationResponse, WorkflowComputingUnitCreationParams, WorkflowComputingUnitMetrics, WorkflowComputingUnitResourceLimit, WorkflowComputingUnitTerminationParams, context}
import edu.uci.ics.texera.service.util.KubernetesClient
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType
import org.jooq.DSLContext

import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ComputingUnitManagingResource {
  private lazy val context: DSLContext = SqlServer
    .getInstance()
    .createDSLContext()

  def userOwnComputingUnit(ctx: DSLContext, cuid: Integer, uid: Integer): Boolean = {
    val computingUnitDao = new WorkflowComputingUnitDao(ctx.configuration())

    Option(computingUnitDao.fetchOneByCuid(uid))
      .exists(_.getUid == uid)
  }

  case class WorkflowComputingUnitCreationParams(
      name: String,
      unitType: String,
      cpuLimit: String,
      memoryLimit: String
  )

  case class WorkflowComputingUnitTerminationParams(uri: String, name: String)

  case class WorkflowComputingUnitResourceLimit(
      cpuLimit: String,
      memoryLimit: String
  )

  case class WorkflowComputingUnitMetrics(
      cpuUsage: String,
      memoryUsage: String
  )

  case class DashboardWorkflowComputingUnit(
      computingUnit: WorkflowComputingUnit,
      uri: String,
      status: String,
      metrics: WorkflowComputingUnitMetrics,
      resourceLimits: WorkflowComputingUnitResourceLimit
  )

  case class TerminationResponse(message: String, uri: String)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit")
class ComputingUnitManagingResource {

  private def getComputingUnitMetrics(cuid: Int): WorkflowComputingUnitMetrics = {
    val metrics: Map[String, String] = KubernetesClient.getPodMetrics(cuid)

    WorkflowComputingUnitMetrics(
      metrics.getOrElse("cpu", ""),
      metrics.getOrElse("memory", "")
    )
  }

  private def getComputingUnitResourceLimit(cuid: Int): WorkflowComputingUnitResourceLimit = {
    val podLimits: Map[String, String] = KubernetesClient.getPodLimits(cuid)

    WorkflowComputingUnitResourceLimit(
      podLimits.getOrElse("cpu", ""),
      podLimits.getOrElse("memory", "")
    )
  }

  /**
    * Create a new pod for the given user ID.
    *
    * @param param The parameters containing the user ID.
    * @return The created pod or an error response.
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/create")
  def createWorkflowComputingUnit(
      param: WorkflowComputingUnitCreationParams
  ): DashboardWorkflowComputingUnit = {
    try {
      withTransaction(context) { ctx =>
        val wcDao = new WorkflowComputingUnitDao(ctx.configuration())

        val computingUnit = new WorkflowComputingUnit()

        computingUnit.setUid(0)
        computingUnit.setName(param.name)
        computingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))

        // Insert using the DAO
        wcDao.insert(computingUnit)

        // Retrieve the generated CUID
        val cuid = ctx.lastID().intValue()
        val insertedUnit = wcDao.fetchOneByCuid(cuid)

        // Create the pod with the generated CUID
        val pod = KubernetesClient.createPod(cuid, param.cpuLimit, param.memoryLimit)

        // Return the dashboard response
        DashboardWorkflowComputingUnit(
          insertedUnit,
          KubernetesClient.generatePodURI(cuid).toString,
          pod.getStatus.getPhase,
          WorkflowComputingUnitMetrics("", ""),
          WorkflowComputingUnitResourceLimit(param.cpuLimit, param.memoryLimit)
        )
      }
    }
  }

  /**
    * List all computing units created by the current user.
    *
    * @return A list of computing units that are not terminated.
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("")
  def listComputingUnits(
                          @Auth user: SessionUser
                        ): List[DashboardWorkflowComputingUnit] = {
    withTransaction(context) { ctx =>
      val computingUnitDao = new WorkflowComputingUnitDao(ctx.configuration())

      val units = computingUnitDao
        .fetchByUid(user.getUid)
        .filter(_.getTerminateTime == null) // Filter out terminated units

      units.map { unit =>
        val cuid = unit.getCuid.intValue()
        val podName = KubernetesClient.generatePodName(cuid)
        val pod = KubernetesClient.getPodByName(podName)

        DashboardWorkflowComputingUnit(
          computingUnit = unit,
          uri = KubernetesClient.generatePodURI(cuid),
          status = pod.map(_.getStatus.getPhase).getOrElse("Unknown"),
          metrics = getComputingUnitMetrics(cuid),
          resourceLimits = getComputingUnitResourceLimit(cuid)
        )
      }.toList
    }
  }

  /**
    * Terminate the computing unit's pod based on the pod URI.
    *
    * @param param The parameters containing the pod URI.
    * @return A response indicating success or failure.
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/terminate")
  def terminateComputingUnit(
                              param: WorkflowComputingUnitTerminationParams,
                              @Auth user: SessionUser,
                            ): TerminationResponse = {
    // Attempt to delete the pod using the provided URI
    val podURI = param.uri
    KubernetesClient.deletePod(podURI)

    // If successful, update the database
    withTransaction(context) { ctx =>
      val cuDao = new WorkflowComputingUnitDao(ctx.configuration())
      val cuid = KubernetesClient.parseCUIDFromURI(podURI)
      val units = cuDao.fetchByCuid(cuid)

      units.forEach(unit => unit.setTerminateTime(new Timestamp(System.currentTimeMillis())))
      cuDao.update(units)
    }

    TerminationResponse(s"Successfully terminated compute unit with URI $podURI", podURI)
  }

  /**
    * Retrieves the CPU and memory metrics for a computing unit identified by its `cuid`.
    *
    * @param cuid The computing unit ID.
    * @return A `WorkflowComputingUnitMetrics` object with CPU and memory usage data.
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}/metrics")
  def getComputingUnitMetrics(
                              @PathParam("cuid") cuid: String,
                              @Auth sessionUser: SessionUser,
                            ): WorkflowComputingUnitMetrics = {
    getComputingUnitMetrics(cuid.toInt)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}/limits")
  def getComputingUnitResourceLimit(
      @PathParam("cuid") cuid: String,
      @Auth user: SessionUser,
  ): WorkflowComputingUnitResourceLimit = {
    getComputingUnitResourceLimit(cuid.toInt)
  }
}
