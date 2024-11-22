package edu.uci.ics.amber.operator

import com.fasterxml.jackson.annotation.JsonProperty

trait PortDescriptor {
  @JsonProperty(required = false)
  var inputPorts: List[PortDescription] = null

  @JsonProperty(required = false)
  var outputPorts: List[PortDescription] = null
}
