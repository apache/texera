package edu.uci.ics.texera.web.model.response

import com.fasterxml.jackson.annotation.JsonProperty

// TODO: change the response, get rid of the message wrapper.
object GenericWebResponse {
  def generateSuccessResponse = new GenericWebResponse(0, "success")
}

class GenericWebResponse() // Default constructor is required for Jackson JSON serialization
{
  private var code = 0
  private var message = ""

  def this(code: Int, message: String) {
    this()
    this.code = code
    this.message = message
  }

  @JsonProperty def getCode: Int = code

  @JsonProperty def getMessage: String = message
}
