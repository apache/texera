package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import org.jooq.types.UInteger

import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, GET, Path, Produces, QueryParam}

case class RuntimeClusterInfo(port: Int)

@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
@Path("/runtime")
class ExecutionRuntimeResource extends LazyLogging {
  @GET
  @Path("/get")
  def getExecutionRuntime(
      @QueryParam("wid") wid: UInteger,
      @QueryParam("uid") uid: UInteger
  ): RuntimeClusterInfo = {
    if (AmberConfig.executionServerMode == "local") {
      RuntimeClusterInfo(8085)
    } else {
      ??? // TODO: add k8s implementation
    }
  }

}
