package edu.uci.ics.texera.service.resource

import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}

@Path("/healthcheck")
@Produces(Array(MediaType.APPLICATION_JSON))
class HealthCheckResource {
  @GET
  def healthCheck: Map[String, String] = Map("status" -> "ok")
}
