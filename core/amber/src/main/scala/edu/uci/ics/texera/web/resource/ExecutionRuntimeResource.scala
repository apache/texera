package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.resource.ExecutionRuntimeResource.clusterMapping
import org.jooq.types.UInteger

import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.{Consumes, GET, POST, Path, Produces, QueryParam}
import scala.collection.mutable

// A class to hold the host and port information
case class ClusterRegistration(key: Long, host: String, port: Int)

object ExecutionRuntimeResource {
  val clusterMapping = new mutable.HashMap[Long, String]()

}
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
@Path("/runtime")
class ExecutionRuntimeResource extends LazyLogging {
  @GET
  @Path("/get")
  def getExecutionRuntime(
      @QueryParam("wid") wid: UInteger,
      @QueryParam("uid") uid: UInteger
  ): String = {
    val key = AmberConfig.executionServerIsolation match {
      case "per-user" =>
        uid.longValue()
      case "per-workflow" =>
        wid.longValue()
      case other =>
        0 // sharing same key
    }
    clusterMapping.getOrElse(key, "")
  }

  @POST
  @Path("/register")
  def reportPort(registration: ClusterRegistration): Response = {
    println(
      s"Received registration for ${registration.key}: ${registration.host}:${registration.port}"
    )
    // Here you could store or log the received host and port as needed
    clusterMapping(registration.key) = s"http://${registration.host}:${registration.port}"
    Response.ok().build()
  }

}
