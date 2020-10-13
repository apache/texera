package edu.uci.ics.texera.web.model.request

import edu.uci.ics.texera.web.model.common.FaultedTupleFrontend

case class SkipTupleRequest(actorPath: String, faultedTuple: FaultedTupleFrontend)
    extends TexeraWsRequest
