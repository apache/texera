package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation

class ReferenceGenome {
  // Required field for selecting a reference genome.
  // This field is mandatory and must be filled by the user.
  @JsonProperty(required = true)
  var referenceGenome: ReferenceGenomeEnum = _

  // Optional field for FastA files.
  // This field is shown only if 'referenceGenome' is set to 'MY_REFERENCE'.
  @JsonSchemaTitle("FastA Files")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "referenceGenome"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^((?!My Reference).)*$"
      )
    )
  )
  val fastAFiles: Option[String] = None

  // Optional field for Gtf files.
  // This field is shown only if 'referenceGenome' is set to 'MY_REFERENCE'.
  @JsonSchemaTitle("Gtf File")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "referenceGenome"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^((?!My Reference).)*$"
      )
    )
  )
  val gtfFile: Option[String] = None
}
