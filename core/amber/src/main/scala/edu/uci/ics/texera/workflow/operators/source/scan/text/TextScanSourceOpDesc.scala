package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.{
  JsonIgnoreProperties,
  JsonProperty,
  JsonPropertyDescription
}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
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
import scala.collection.convert.ImplicitConversions.`iterator asScala`

/* ignoring inherited limit and offset properties because they are not hideable */
@JsonIgnoreProperties(value = Array("limit", "offset"))
class TextScanSourceOpDesc extends ScanSourceOpDesc {

  /* create new, identical limit and offset fields
      with additional annotations to make hideable
      TODO: to be considered in potential future refactor */

  @JsonProperty()
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("max output count")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "outputAsSingleTuple"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    )
  )
  var limitHideable: Option[Int] = None

  @JsonProperty()
  @JsonSchemaTitle("Offset")
  @JsonPropertyDescription("starting point of output")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "outputAsSingleTuple"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    )
  )
  var offsetHideable: Option[Int] = None

  @JsonProperty(defaultValue = "false")
  @JsonPropertyDescription(
    "scan entire text file into single output tuple, ignoring any offsets and limits"
  )
  var outputAsSingleTuple: Boolean = false

  fileTypeName = Option("Text")

  @Override
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    filePath match {
      case Some(path) =>
        // get offset and max line values
        val reader = new BufferedReader(new FileReader(path))
        val offsetValue = offsetHideable.getOrElse(0)
        var lines = reader.lines().iterator().drop(offsetValue)
        if (limitHideable.isDefined) {
          lines = lines.take(limitHideable.get)
        }
        val count: Int = lines.map(_ => 1).sum
        reader.close()

        // using only 1 worker for text scan to maintain proper ordering
        OpExecConfig.localLayer(
          operatorIdentifier,
          _ => {
            val startOffset: Int = offsetValue
            val endOffset: Int = offsetValue + count
            new TextScanSourceOpExec(this, startOffset, endOffset, outputAsSingleTuple)
          }
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  @Override
  override def inferSchema(): Schema = {
    Schema
      .newBuilder()
      .add(new Attribute(if (outputAsSingleTuple) "file" else "line", AttributeType.STRING))
      .build()
  }
}
