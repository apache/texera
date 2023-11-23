package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

/**
  * TextSourceOpDesc is a trait holding commonly used properties and functions
  * used for variations of text input processing
  */
trait TextSourceOpDesc {
  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  var attributeType: FileAttributeType = FileAttributeType.STRING

  @JsonProperty(defaultValue = "line", required = true)
  @JsonSchemaTitle("Attribute Name")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var attributeName: String = "line"

  @JsonSchemaTitle("Limit")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "^binary$|^single string$")
    )
  )
  var fileScanLimit: Option[Int] = None

  @JsonSchemaTitle("Offset")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "^binary$|^single string$")
    )
  )
  var fileScanOffset: Option[Int] = None
}
