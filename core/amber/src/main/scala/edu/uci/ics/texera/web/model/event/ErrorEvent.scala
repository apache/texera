package edu.uci.ics.texera.web.model.event

import edu.uci.ics.backenderror.Error

case class ErrorEvent(errorMap: Map[String,String]) extends TexeraWebSocketEvent
