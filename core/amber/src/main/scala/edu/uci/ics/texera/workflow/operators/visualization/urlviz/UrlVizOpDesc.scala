package edu.uci.ics.texera.workflow.operators.visualization.urlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer

/**
  * URL Visualization operator to render any content in given URL link
  * This is the description of the operator
  */
@JsonSchemaInject(json = """
 {
   "attributeTypeRules": {
     "urlContentAttrName": {
       "enum": ["string"]
     }
   }
 }
 """)
class UrlVizOpDesc extends VisualizationOperator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL content")
  @AutofillAttributeName
  private val urlContentAttrName: String = ""

  override def chartType: String = VisualizationConstants.HTML_VIZ

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp =
    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _, _) => new UrlVizOpExec(urlContentAttrName, operatorSchemaInfo))
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "URL visualizer",
      "Render the content of URL",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build

}
