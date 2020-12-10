package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo}
import edu.uci.ics.texera.workflow.common.{Utils, WorkflowContext}
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path, Produces}

import scala.collection.{JavaConverters}

case class SchemaPropagationResponse(code: Int, result: Map[String, List[List[String]]], message: String)

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
    println("workflow", workflow)
    val context = new WorkflowContext
    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(workflow.operators, workflow.links, workflow.breakpoints),
      context
    )
    try {
      val schemaPropagationResult = texeraWorkflowCompiler
        .propagateWorkflowSchema()
        .map(e => {
          println("e", e)
          (e._1.operatorID, List(JavaConverters.asScalaBuffer(e._2.getAttributeNames).toList, JavaConverters.asScalaBuffer(e._2.getAttributeTypes).toList))
        })
      println("schemaPropagationResult", schemaPropagationResult)
      schemaPropagationResult.foreach(element=>{
        println("element",element)
      })

      SchemaPropagationResponse(0, schemaPropagationResult, null)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        SchemaPropagationResponse(1, null, e.getMessage)
    }
  }

}
