package edu.uci.ics.texera.workflow.operators.download

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}

class BulkDownloaderOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL Attribute")
  @JsonPropertyDescription(
    "Only accepts standard URL format"
  )
  @AutofillAttributeName
  var urlAttribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Result Attribute")
  @JsonPropertyDescription(
    "Attribute name for results(downloaded file paths)"
  )
  var resultAttribute: String = _

  override def operatorExecutor(
      executionId: Long,
      operatorSchemaInfo: OperatorSchemaInfo
  ): OpExecConfig = {
    assert(getContext.userId.isDefined)
    OpExecConfig.oneToOneLayer(
      executionId,
      operatorIdentifier,
      OpExecInitInfo(_ =>
        new BulkDownloaderOpExec(
          getContext,
          urlAttribute,
          resultAttribute,
          operatorSchemaInfo
        )
      )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Bulk Downloader",
      operatorDescription = "Download urls in a string column",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    val inputSchema = schemas(0)
    val outputSchemaBuilder = Schema.newBuilder
    // keep the same schema from input
    outputSchemaBuilder.add(inputSchema)
    if (resultAttribute == null || resultAttribute.isEmpty) {
      resultAttribute = urlAttribute + " result"
    }
    outputSchemaBuilder.add(
      new Attribute(
        resultAttribute,
        AttributeType.STRING
      )
    )
    outputSchemaBuilder.build
  }
}
