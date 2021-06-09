package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.InputPort
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo
import edu.uci.ics.texera.workflow.common.metadata.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator
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

  override def operatorExecutor =
    new HtmlVizOpExecConfig(this.operatorIdentifier, htmlContentAttrName)

  override def operatorInfo =
    new OperatorInfo(
      "HTML visualizer",
      "Render the result of HTML content",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      asScalaBuffer(singletonList(new InputPort("", false))).toList,
      asScalaBuffer(singletonList(new OutputPort(""))).toList
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema.newBuilder.add(new Attribute("HTML-content", AttributeType.STRING)).build
}
