package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation

class ReferenceGenome {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Select Reference Genome")
  @JsonPropertyDescription("reference genome for sequence alignment")
  var referenceGenome: ReferenceGenomeEnum = _

  @JsonSchemaTitle("FastA Files")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "referenceGenome"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^((?!others).)*$"
      ) // regex to hide when "others" is not present
    )
  )
  val fastAFiles: Option[String] = None

  @JsonSchemaTitle("Gtf File")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "referenceGenome"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^((?!others).)*$"
      ) // regex to hide when "others" is not present
    )
  )
  val gtfFile: Option[String] = None
}
