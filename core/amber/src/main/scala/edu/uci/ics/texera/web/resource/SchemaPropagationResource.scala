package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.workflow.common.workflow.{WorkflowInfo, WorkflowCompiler}
import edu.uci.ics.texera.workflow.common.{Utils, WorkflowContext}
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path, Produces}

import scala.collection.JavaConverters

case class SchemaPropagationResponse(code: Int, result: Map[String, List[String]], message: String)

@Path("/queryplan")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class SchemaPropagationResource {

  @POST
  @Path("/autocomplete")
  def suggestAutocompleteSchema(
      workflowStr: String
  ): SchemaPropagationResponse = {
    println(workflowStr)
    val workflow = Utils.objectMapper.readValue(workflowStr, classOf[WorkflowInfo])
    val context = new WorkflowContext
    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(workflow.operators, workflow.links, workflow.breakpoints),
      context
    )
    try {
      val schemaPropagationResult = texeraWorkflowCompiler
        .propagateWorkflowSchema()
        .map(e => (e._1.operatorID, JavaConverters.asScalaBuffer(e._2.getAttributeNames).toList))
      SchemaPropagationResponse(0, schemaPropagationResult, null)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        SchemaPropagationResponse(1, null, e.getMessage)
    }
  }

}
