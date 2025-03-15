package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.service.ResultExportService
import io.dropwizard.auth.Auth

import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.jdk.CollectionConverters._

@Path("/result")
@Produces(Array(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM))
class ResultResource extends LazyLogging {

  @POST
  @Path("/export")
  def exportResult(
      request: ResultExportRequest,
      @Auth user: SessionUser
  ): Response = {

    if (request.operatorIds.size <= 0)
      Response
        .status(Response.Status.BAD_REQUEST)
        .`type`(MediaType.APPLICATION_JSON)
        .entity(Map("error" -> "No operator selected").asJava)
        .build()

    try {
      request.destination match {
        case "local" =>
          // CASE A: multiple operators => produce ZIP
          if (request.operatorIds.size > 1) {
            val resultExportService = new ResultExportService(WorkflowIdentity(request.workflowId))
            val (zipStream, zipFileNameOpt) =
              resultExportService.exportOperatorsAsZip(user.user, request)

            if (zipStream == null) {
              throw new RuntimeException("Zip stream is null")
            }

            val finalFileName = zipFileNameOpt.getOrElse("operators.zip")
            return Response
              .ok(zipStream, "application/zip")
              .header("Content-Disposition", "attachment; filename=\"" + finalFileName + "\"")
              .build()
          }

          // CASE B: exactly one operator => single file
          if (request.operatorIds.size != 1) {
            return Response
              .status(Response.Status.BAD_REQUEST)
              .`type`(MediaType.APPLICATION_JSON)
              .entity(Map("error" -> "Local download supports no operator or many.").asJava)
              .build()
          }
          val singleOpId = request.operatorIds.head

          val resultExportService = new ResultExportService(WorkflowIdentity(request.workflowId))
          val (streamingOutput, fileNameOpt) =
            resultExportService.exportOperatorResultAsStream(request, singleOpId)

          if (streamingOutput == null) {
            return Response
              .status(Response.Status.INTERNAL_SERVER_ERROR)
              .`type`(MediaType.APPLICATION_JSON)
              .entity(Map("error" -> "Failed to export operator").asJava)
              .build()
          }

          val finalFileName = fileNameOpt.getOrElse("download.dat")
          Response
            .ok(streamingOutput, MediaType.APPLICATION_OCTET_STREAM)
            .header("Content-Disposition", "attachment; filename=\"" + finalFileName + "\"")
            .build()
        case _ =>
          // destination = "dataset" by default
          val resultExportService = new ResultExportService(WorkflowIdentity(request.workflowId))
          val exportResponse = resultExportService.exportResult(user.user, request)
          Response.ok(exportResponse).build()
      }
    } catch {
      case ex: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .`type`(MediaType.APPLICATION_JSON)
          .entity(Map("error" -> ex.getMessage).asJava)
          .build()
    }
  }
}
