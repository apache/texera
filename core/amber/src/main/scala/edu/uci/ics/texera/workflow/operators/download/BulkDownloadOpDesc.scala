package edu.uci.ics.texera.workflow.operators.download

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}

class BulkDownloadOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL Attribute")
  @JsonPropertyDescription(
    "Should follow standard URL format: protocol+hostname+filename"
  )
  @AutofillAttributeName
  var urlAttribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Result Attribute")
  @JsonPropertyDescription(
    "Attribute name for results(downloaded file paths)"
  )
  var resultAttribute: String = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    assert(context.userId.isDefined)
    new OneToOneOpExecConfig(
      operatorIdentifier,
      _ =>
        new BulkDownloadOpExec(
          context.userId.get,
          urlAttribute,
          resultAttribute,
          operatorSchemaInfo
        )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Bulk Download",
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
