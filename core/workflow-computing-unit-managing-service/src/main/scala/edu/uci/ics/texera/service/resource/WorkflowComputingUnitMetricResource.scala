package edu.uci.ics.texera.service.resource

import edu.uci.ics.texera.service.resource.WorkflowComputingUnitMetricResource.WorkflowComputingUnitMetrics
import edu.uci.ics.texera.service.util.KubernetesMetricService._
import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType

object WorkflowComputingUnitMetricResource {

  case class WorkflowComputingUnitMetrics (
    cuid: Int,
    cpu: Double,
    memory: Double
  )
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/resource-metrics")
class WorkflowComputingUnitMetricResource {

  /**
   * Retrieves the computing unit metrics for a given name in the specified namespace.
   *
   * @param computingUnitCUID  The computing units uid for retrieving metrics
   * @return The computing unit metrics for a given name in a specified namespace
   */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/pod-metrics")
  def getComputingUnitMetric(@QueryParam("computingUnitCUID") computingUnitCUID: Int): WorkflowComputingUnitMetrics = {
    println(computingUnitCUID)
    val metrics: Map[String, Any] = getPodMetrics(computingUnitCUID)

    WorkflowComputingUnitMetrics(
      cuid = computingUnitCUID,
      cpu = metrics.get("cpu").collect { case value: Double => value }.getOrElse(0.0),
      memory = metrics.get("memory").collect { case value: Double => value }.getOrElse(0.0)
    )
  }

}
