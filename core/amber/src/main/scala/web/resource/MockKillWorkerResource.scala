package web.resource

import java.nio.file.Files

import Engine.Common.AmberMessage.ControlMessage.KillAndRecover
import com.fasterxml.jackson.databind.JsonNode
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, POST, Path, Produces}
import texera.common.TexeraUtils
import texera.common.schema.{GroupOrder, OperatorGroupConstants, OperatorSchemaGenerator}
import web.model.event.RecoveryStartedEvent

import scala.beans.BeanProperty
import scala.collection.JavaConverters

@Path("/kill")
@Produces(Array(MediaType.APPLICATION_JSON))
class MockKillWorkerResource() {

  case class OperatorMetadata
  (
    @BeanProperty operators: List[JsonNode],
    @BeanProperty groups: List[GroupOrder]
  )

  @POST
  @Path("/worker") def mockKillWorker: Unit = {
    WorkflowWebsocketResource.sessionJobs.foreach(p => {
      val controller = p._2._2
      Thread.sleep(1500)
      controller ! KillAndRecover
    })
  }

}
