package edu.uci.ics.texera.web.model.request

case class RegisterWIdRequest(wId: String, recoverFrontendState: Boolean)
    extends TexeraWebSocketRequest

case class HeartBeatRequest() extends TexeraWebSocketRequest
