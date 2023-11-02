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
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.{FileDecodingMethod, ScanSourceOpDesc}

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/* ignoring inherited limit, offset, and fileEncoding properties because they are not hideable
 *   new, identical fields are created with additional annotations to make hideable
 *
 *
 *   limit and offset are created in the TextSourceOpDesc trait
 *   TODO: to be considered in potential future refactor*/
@JsonIgnoreProperties(value = Array("limit", "offset", "fileEncoding"))
class TextScanSourceOpDesc extends ScanSourceOpDesc with TextSourceOpDesc {
  // hide the fileEncoding dropdown for BINARY AttributeType, it is unused
  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("File Encoding")
  @JsonPropertyDescription("decoding charset to use on input")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "^binary$")
    )
  )
  var fileEncodingHideable: FileDecodingMethod = FileDecodingMethod.UTF_8

  // indicates the AttributeType of output tuple(s) - supports all AttributeType except ANY
  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  @JsonPropertyDescription("Attribute type of output tuple(s)")
  var attributeType: TextScanSourceAttributeType = TextScanSourceAttributeType.STRING

  fileTypeName = Option("Text")

  @Override
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    filePath match {
      case Some(path) =>
        // get offset and max line values, unused if in output single tuple mode (i.e. binary or string as single tuple)
        val offsetValue = offsetHideable.getOrElse(0)
        var count: Int = 1

        if (!attributeType.isOutputSingleTuple) {
          // count number of rows in input text file
          val reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(path), fileEncodingHideable.getCharset)
          )
          count = countNumLines(reader.lines().iterator().asScala, offsetValue)
          reader.close()
        }

        // default attribute name
        val defaultAttributeName: String = if (attributeType.isOutputSingleTuple) "file" else "line"

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
    val defaultAttributeName: String = if (attributeType.isOutputSingleTuple) "file" else "line"
    Schema
      .newBuilder()
      .add(
        new Attribute(
          if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
          else attributeName.get,
          attributeType.getType
        )
      )
      .build()
  }
}
