package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import javax.websocket.Session

class WebsocketSubscriber(session:Session)extends Observer[TexeraWebSocketEvent]{
  def onNext(evt:TexeraWebSocketEvent):Unit ={
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
  }
}
