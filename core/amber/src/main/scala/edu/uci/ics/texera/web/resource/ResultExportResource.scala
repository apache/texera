package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.service.WorkflowService
import io.dropwizard.auth.Auth

import javax.ws.rs._
import javax.ws.rs.core.Response
import scala.jdk.CollectionConverters._

@Path("/export")
class ResultExportResource extends LazyLogging {

  @POST
  @Path("/result")
  def exportResult(
      request: ResultExportRequest,
      @Auth user: SessionUser
  ): Response = {

    try {
      val workflowState = WorkflowService.getOrCreate(WorkflowIdentity(request.workflowId))
      if (workflowState == null) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(s"Workflow ID ${request.workflowId} not found.")
          .build()
      }

      val exportResponse: ResultExportResponse =
        workflowState.exportService.exportResult(user.user, request)

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
