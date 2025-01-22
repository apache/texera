package edu.uci.ics.texera.service.resource

import edu.uci.ics.texera.service.resource.WorkflowComputingUnitMetricResource.WorkflowComputingUnitMetricParam
import edu.uci.ics.texera.service.util.KubernetesMetricService._
import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType

object WorkflowComputingUnitMetricResource {

  case class WorkflowComputingUnitMetricParam(computingUnitName: String, namespace: String)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/resource-metrics")
class WorkflowComputingUnitMetricResource {

  /**
   * Retrieves the computing unit metrics for a given name in the specified namespace.
   *
   * @param params  The parameters containing the computingUnitName and namespace
   * @return The computing unit metrics for a given name in a specified namespace
   */
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/pod-metrics")
  def getComputingUnitMetric(params: WorkflowComputingUnitMetricParam): Map[String, String] = {
    getPodMetrics(params.computingUnitName, params.namespace)
  }

}
