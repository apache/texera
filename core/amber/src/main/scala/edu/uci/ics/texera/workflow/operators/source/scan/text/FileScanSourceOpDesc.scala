package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaString, JsonSchemaTitle}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.{FileDecodingMethod, ScanSourceOpDesc}

class FileScanSourceOpDesc extends ScanSourceOpDesc with TextSourceOpDesc {
  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("Encoding")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "binary")
    )
  )
  var encoding: FileDecodingMethod = FileDecodingMethod.UTF_8

  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  var attributeType: AttributeType = AttributeType.STRING

  fileTypeName = Option("")

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    OpExecConfig.localLayer(operatorIdentifier, _ => new FileScanSourceOpExec(this))

  override def inferSchema(): Schema = {
    new Schema(new Attribute(attributeName, attributeType))
  }
}
