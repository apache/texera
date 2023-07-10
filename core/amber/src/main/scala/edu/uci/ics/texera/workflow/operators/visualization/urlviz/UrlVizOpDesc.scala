package edu.uci.ics.texera.workflow.operators.visualization.urlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, util}
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
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.visualization.htmlviz.HtmlVizOpExec
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
class UrlVizOpDesc extends VisualizationOperator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @AutofillAttributeName
  var urlContentAttrName: String = _

  override def chartType: String = VisualizationConstants.HTML_VIZ

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) =
    throw new UnsupportedOperationException("opExec implemented in operatorExecutorMultiLayer")
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "URL visualizer",
      "Render the content of URL",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  override def operatorExecutorMultiLayer(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val partialId = util.makeLayer(operatorIdentifier, "url")
    val partialLayer = OpExecConfig
      .oneToOneLayer(
        operatorIdentifier,
        _ => new UrlVizOpPartialExec(urlContentAttrName, operatorSchemaInfo)
      )
      .withId(partialId)
      .withNumWorkers(1)
    val finalId = util.makeLayer(operatorIdentifier, "html")
    val finalLayer = OpExecConfig
      .oneToOneLayer(operatorIdentifier, _ => new HtmlVizOpExec("html-content", operatorSchemaInfo))
      .withId(finalId)
    val layers = Array(partialLayer, finalLayer)
    val links = Array(new LinkIdentity(partialLayer.id, finalLayer.id))
    PhysicalPlan.apply(layers, links)
  }
  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
}
