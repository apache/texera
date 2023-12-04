package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.web.model.common.FaultedTupleFrontend

case class SkipTupleRequest(workerIds: Array[String])
    extends TexeraWebSocketRequest
