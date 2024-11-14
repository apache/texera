package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import org.jooq.types.UInteger

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, PathParam, Produces}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/runtime")
class ExecutionRuntimeResource extends LazyLogging {
  @GET
  @Path("/get/{wid}")
  def getExecutionRuntime(@PathParam("wid") wid: UInteger): String = {
    if (AmberConfig.executionServerMode == "local") {
      if (AmberConfig.executionServerIsolation == "shared") {
        "http://localhost:8085/"
      } else {
        ??? // TODO: create execution cluster on-demand
      }
    } else {
      ??? // TODO: spawn pods on k8s
    }
  }
}
