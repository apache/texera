package edu.uci.ics.texera.web.model.event

case class ResultDownloadResponse(downloadType: String, link: String, message: String)
    extends TexeraWebSocketEvent
