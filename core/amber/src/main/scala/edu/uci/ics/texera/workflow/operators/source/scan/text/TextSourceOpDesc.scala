package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation

/**
  * TextSourceOpDesc is a trait holding commonly used properties and functions
  * used for variations of text input processing
  */
trait TextSourceOpDesc {
  /* create new, identical limit and offset fields
      with additional annotations to make hideable

      binary attributes and strings that are in outputAsSingleTuple mode
      will always read the entire input, so limit / offset are disabled in these cases
   */

  @JsonProperty()
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("max output count")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^string [(]entire input in 1 tuple[)]$"
      )
    )
  )
  var limitHideable: Option[Int] = None

  @JsonProperty()
  @JsonSchemaTitle("Offset")
  @JsonPropertyDescription("starting point of output")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^string [(]entire input in 1 tuple[)]$"
      )
    )
  )
  var offsetHideable: Option[Int] = None

  // optional field allowing users to specify name of resulting output tuple attribute
  @JsonProperty()
  @JsonSchemaTitle("Output Attribute Name")
  @JsonPropertyDescription("optionally specify output attribute name")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var attributeName: Option[String] = None

  def countNumLines(linesIterator: Iterator[String], offsetValue: Int): Int = {
    var lines = linesIterator.drop(offsetValue)
    if (limitHideable.isDefined) {
      lines = lines.take(limitHideable.get)
    }
    lines.map(_ => 1).sum
  }
}
