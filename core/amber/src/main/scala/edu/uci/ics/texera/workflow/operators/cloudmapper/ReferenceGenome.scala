package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle


class ReferenceGenome {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Select Reference Genome")
    @JsonPropertyDescription("reference genome for sequence alignment")
    var referenceGenome: ReferenceGenomeEnum = _
}
