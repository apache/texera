package edu.uci.ics.texera.web.model.event

case class ResultDownloadResponse(downloadType: String, message: String) extends TexeraWebSocketEvent
