package edu.uci.ics.amber.operator.visualization.urlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode

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
class UrlVizOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL content")
  @AutofillAttributeName
  val urlContentAttrName: String = ""

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.visualization.urlviz.UrlVizOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => {
          val outputSchema = Schema
            .builder()
            .add(new Attribute("html-content", AttributeType.STRING))
            .build()
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "URL visualizer",
      "Render the content of URL",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

}
