package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.{
  JsonIgnoreProperties,
  JsonProperty,
  JsonPropertyDescription
}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileReader}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/* ignoring inherited limit and offset properties because they are not hideable
 *   new, identical limit and offset fields with additional annotations to make hideable
 *   are created and used from the TextSourceOpDesc trait
 *   TODO: to be considered in potential future refactor*/
@JsonIgnoreProperties(value = Array("limit", "offset"))
class TextScanSourceOpDesc extends ScanSourceOpDesc with TextSourceOpDesc {

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Binary")
  @JsonPropertyDescription("output as Binary instead of UTF-8")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "outputAsSingleTuple"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  var outputAsBinary: Boolean = false

  fileTypeName = Option("Text")

  @Override
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    filePath match {
      case Some(path) =>
        // get offset and max line values
        val reader = new BufferedReader(new FileReader(path))
        val offsetValue = offsetHideable.getOrElse(0)
        val count: Int = countNumLines(reader.lines().iterator().asScala, offsetValue)
        reader.close()

        // default attribute name
        val defaultAttributeName: String = if (outputAsSingleTuple) "file" else "line"

        // using only 1 worker for text scan to maintain proper ordering
        OpExecConfig.localLayer(
          operatorIdentifier,
          _ => {
            val startOffset: Int = offsetValue
            val endOffset: Int = offsetValue + count
            new TextScanSourceOpExec(
              this,
              startOffset,
              endOffset,
              if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
              else attributeName.get
            )
          }
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  @Override
  override def inferSchema(): Schema = {
    val defaultAttributeName: String = if (outputAsSingleTuple) "file" else "line"
    Schema
      .newBuilder()
      .add(
        new Attribute(
          if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
          else attributeName.get,
          AttributeType.STRING
        )
      )
      .build()
  }
}
