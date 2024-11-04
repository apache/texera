package edu.uci.ics.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.amber.operator.metadata.annotation.HideAnnotation
import edu.uci.ics.amber.operator.source.scan.FileAttributeType

/**
  * TextSourceOpDesc is a trait holding commonly used properties and functions used for variations of text input processing
  * Create new, identical limit and offset fields with additional annotations to make hideable binary attributes
  * and strings that are in SingleTuple mode will always read the entire input, so limit / offset are disabled in these cases
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
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanLimit: Option[Int] = None

  @JsonSchemaTitle("Offset")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanOffset: Option[Int] = None
}
