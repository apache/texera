package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
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
  * HTML Visualization operator to render any given HTML code
  * This is the description of the operator
  */
class HtmlVizOpDesc extends VisualizationOperator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("HTML content")
  @AutofillAttributeName var htmlContentAttrName: String = _

  override def chartType: String = VisualizationConstants.HTML_VIZ

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp =
    PhysicalOp.oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecInitInfo((_, _, _) => new HtmlVizOpExec(htmlContentAttrName, operatorSchemaInfo))
    )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "HTML visualizer",
      "Render the result of HTML content",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
}
