package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.request.TexeraWebSocketRequest
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject

import scala.reflect.{ClassTag, classTag}

class WebsocketInput(errorHandler: Throwable => Unit) {
  private val wsInput = PublishSubject.create[(TexeraWebSocketRequest, Option[Integer])]()

  def subscribe[T <: TexeraWebSocketRequest: ClassTag](
      callback: (T, Option[Integer]) => Unit
  ): Disposable = {
    wsInput.subscribe((evt: (TexeraWebSocketRequest, Option[Integer])) => {
      evt._1 match {
        case req: T if classTag[T].runtimeClass.isInstance(req) =>
          try {
            callback(req, evt._2)
          } catch {
            case throwable: Throwable =>
              errorHandler(throwable)
          }
        case other =>
        // skip this one because it doesn't match the type we want
      }
    })
  }

  def onNext(req: TexeraWebSocketRequest, uidOpt: Option[Integer]): Unit = {
    wsInput.onNext((req, uidOpt))
  }

}
