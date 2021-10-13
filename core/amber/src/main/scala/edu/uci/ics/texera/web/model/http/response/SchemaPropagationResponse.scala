package edu.uci.ics.texera.web.model.http.response

import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute

case class SchemaPropagationResponse(
    code: Int,
    result: Map[String, List[Option[List[Attribute]]]],
    message: String
)
