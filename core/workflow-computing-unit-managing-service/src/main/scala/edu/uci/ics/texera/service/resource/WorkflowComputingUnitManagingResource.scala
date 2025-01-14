package edu.uci.ics.texera.service.resource

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowComputingUnitDao
import edu.uci.ics.texera.dao.jooq.generated.tables.WorkflowComputingUnit.WORKFLOW_COMPUTING_UNIT
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import edu.uci.ics.texera.service.resource.WorkflowComputingUnitManagingResource.{
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitCreationParams,
  WorkflowComputingUnitTerminationParams,
  TerminationResponse,
  context
}
import edu.uci.ics.texera.service.util.KubernetesClientService
import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.sql.Timestamp

object WorkflowComputingUnitManagingResource {

  private lazy val context: DSLContext = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()

  case class WorkflowComputingUnitCreationParams(name: String, unitType: String)

  case class WorkflowComputingUnitTerminationParams(uri: String, name: String)

  case class DashboardWorkflowComputingUnit(
      computingUnit: WorkflowComputingUnit,
      uri: String,
      status: String
  )

  case class TerminationResponse(message: String, uri: String)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit")
class WorkflowComputingUnitManagingResource {

  /**
    * Create a new pod for the given user ID.
    *
    * @param param The parameters containing the user ID.
    * @return The created pod or an error response.
    */
  @POST
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

        computingUnit.setUid(UInteger.valueOf(0))
        computingUnit.setName(param.name)
        computingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))

        // Insert using the DAO
        wcDao.insert(computingUnit)

        // Retrieve the generated CUID
        val cuid = ctx.lastID().intValue()
        val insertedUnit = wcDao.fetchOneByCuid(UInteger.valueOf(cuid))

        // Create the pod with the generated CUID
        val pod = KubernetesClientService.createPod(cuid)

        // Return the dashboard response
        DashboardWorkflowComputingUnit(
          insertedUnit,
          KubernetesClientService.generatePodURI(cuid).toString,
          pod.getStatus.getPhase
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
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("")
  def listComputingUnits(): java.util.List[DashboardWorkflowComputingUnit] = {
    withTransaction(context) { ctx =>
      val result = ctx
        .select()
        .from(WORKFLOW_COMPUTING_UNIT)
        .where(WORKFLOW_COMPUTING_UNIT.TERMINATE_TIME.isNull) // Filter out terminated units
        .fetch()
        .map(record => {
          val unit = record.into(WORKFLOW_COMPUTING_UNIT).into(classOf[WorkflowComputingUnit])
          val cuid = unit.getCuid.intValue()
          val podName = KubernetesClientService.generatePodName(cuid)
          val pod = KubernetesClientService.getPodByName(podName)

          DashboardWorkflowComputingUnit(
            computingUnit = unit,
            uri = KubernetesClientService.generatePodURI(cuid).toString,
            status = if (pod != null && pod.getStatus != null) pod.getStatus.getPhase else "Unknown"
          )
        })

      result
    }
  }

  /**
    * Terminate the computing unit's pod based on the pod URI.
    *
    * @param param The parameters containing the pod URI.
    * @return A response indicating success or failure.
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/terminate")
  def terminateComputingUnit(param: WorkflowComputingUnitTerminationParams): TerminationResponse = {
    // Attempt to delete the pod using the provided URI
    val podURI = param.uri
    KubernetesClientService.deletePod(podURI)

    // If successful, update the database
    withTransaction(context) { ctx =>
      val cuDao = new WorkflowComputingUnitDao(ctx.configuration())
      val cuid = KubernetesClientService.parseCUIDFromURI(podURI)
      val units = cuDao.fetchByCuid(UInteger.valueOf(cuid))

      units.forEach(unit => unit.setTerminateTime(new Timestamp(System.currentTimeMillis())))
      cuDao.update(units)
    }

    TerminationResponse(s"Successfully terminated compute unit with URI $podURI", podURI)
  }
}
