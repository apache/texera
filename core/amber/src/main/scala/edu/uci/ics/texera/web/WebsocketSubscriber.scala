package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable

import javax.websocket.Session

class WebsocketSubscriber(session: Session) extends Observer[TexeraWebSocketEvent] {
  override def onNext(evt: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
  }

  // not implemented
  override def onSubscribe(d: Disposable): Unit = ???

  override def onError(e: Throwable): Unit = ???

  override def onComplete(): Unit = ???
}
