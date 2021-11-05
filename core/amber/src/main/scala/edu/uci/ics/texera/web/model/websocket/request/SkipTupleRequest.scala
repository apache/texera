package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.web.model.common.FaultedTupleFrontend

case class SkipTupleRequest(actorPath: String, faultedTuple: FaultedTupleFrontend)
    extends TexeraWebSocketRequest
