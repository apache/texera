package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.service.ResultExportService
import io.dropwizard.auth.Auth

import javax.ws.rs._
import javax.ws.rs.core.Response
import scala.jdk.CollectionConverters._

@Path("/result")
class ResultResource extends LazyLogging {

  @POST
  @Path("/export")
  def exportResult(
      request: ResultExportRequest,
      @Auth user: SessionUser
  ): Response = {

    try {
      val resultExportService = new ResultExportService(WorkflowIdentity(request.workflowId))

      val exportResponse: ResultExportResponse =
        resultExportService.exportResult(user.user, request)

      Response.ok(exportResponse).build()

    } catch {
      case ex: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map("error" -> ex.getMessage).asJava)
          .build()
    }
  }

}
