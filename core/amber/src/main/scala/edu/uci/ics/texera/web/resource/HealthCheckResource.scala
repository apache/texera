package edu.uci.ics.texera.web.resource

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}

@Path("/healthcheck")
@Produces(Array(MediaType.APPLICATION_JSON))
class HealthCheckResource {
  @GET
  def healthCheck: Map[String, String] = Map("status" -> "ok")
}
