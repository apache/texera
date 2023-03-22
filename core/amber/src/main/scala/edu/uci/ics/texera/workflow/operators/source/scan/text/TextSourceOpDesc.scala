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
      with additional annotations to make hideable */

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
    "scan entire text into single output tuple, ignoring any offsets and limits"
  )
  var outputAsSingleTuple: Boolean = false

  def countNumLines(linesIterator: Iterator[String], offsetValue: Int): Int = {
    var lines = linesIterator.drop(offsetValue)
    if (limitHideable.isDefined) {
      lines = lines.take(limitHideable.get)
    }
    lines.map(_ => 1).sum
  }
}
