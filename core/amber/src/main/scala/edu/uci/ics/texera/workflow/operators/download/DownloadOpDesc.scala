package edu.uci.ics.texera.workflow.operators.download

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

class DownloadOpDesc extends OperatorDescriptor {

  var attributes: List[DownloadAttributeUnit] = List()

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(
      operatorIdentifier,
      _ => new DownloadOpExec(attributes, operatorSchemaInfo)
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Download",
      operatorDescription = "Download a url in a string column",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    Preconditions.checkArgument(attributes.nonEmpty)
    val outputSchemaBuilder = Schema.newBuilder
    for (attribute <- attributes) {
      outputSchemaBuilder.add(
        new Attribute(
          attribute.getUrlAttribute(),
          schemas(0).getAttribute(attribute.getUrlAttribute()).getType
        )
      )
      outputSchemaBuilder.add(
        new Attribute(
          attribute.getResultAttribute(),
          AttributeType.STRING
        )
      )
    }
    outputSchemaBuilder.build
  }
}
