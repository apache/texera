package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import javax.websocket.Session
import rx.lang.scala.Observer

class WebsocketSubscriber(session: Session) extends Observer[TexeraWebSocketEvent] {
  override def onNext(evt: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
  }
}
