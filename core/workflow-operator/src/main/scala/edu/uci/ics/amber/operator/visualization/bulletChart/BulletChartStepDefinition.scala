package edu.uci.ics.amber.operator.visualization.bulletChart

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle

/**
  * Defines a step range used for qualitative segments in the Bullet Chart.
  */

class BulletChartStepDefinition @JsonCreator() (
    @JsonProperty("start")
    @JsonSchemaTitle("Start")
    var start: String,
    @JsonProperty("end")
    @JsonSchemaTitle("End")
    var end: String
)
