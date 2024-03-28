package edu.uci.ics.texera.workflow.operators.source.scan

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.text.TextSourceOpDesc

@JsonIgnoreProperties(value = Array("limit", "offset", "fileEncoding"))
class FileScanSourceOpDesc extends ScanSourceOpDesc with TextSourceOpDesc {
  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("Encoding")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "binary")
    )
  )
  var encoding: FileDecodingMethod = FileDecodingMethod.UTF_8

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Extract")
  var extract: Boolean = false

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Include Filename")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "extract"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  var outputFileName: Boolean = false

  fileTypeName = Option("")

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val (filepath, fileDesc) = determineFilePathOrDesc()
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) =>
          new FileScanSourceOpExec(
            filepath,
            fileDesc,
            attributeType,
            encoding,
            extract,
            outputFileName,
            fileScanLimit,
            fileScanOffset
          )
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> inferSchema()))
      )
  }

  override def inferSchema(): Schema = {
    val builder = Schema.builder()
    if (outputFileName) builder.add(new Attribute("filename", AttributeType.STRING))
    builder.add(new Attribute(attributeName, attributeType.getType)).build()
  }
}
