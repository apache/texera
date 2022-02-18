package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.request.TexeraWebSocketRequest
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject
import org.jooq.types.UInteger
import scala.compat.java8.FunctionConverters._

import scala.reflect.{ClassTag, classTag}

class WebsocketInput(errorHandler: Throwable => Unit) {
  private val wsInput = PublishSubject.create[(TexeraWebSocketRequest, Option[UInteger])]()
  def subscribe[T <: TexeraWebSocketRequest: ClassTag](
      callback: (T, Option[UInteger]) => Unit
  ): Disposable = {
    wsInput.subscribe((evt: (TexeraWebSocketRequest, Option[UInteger])) => {
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

  def onNext(req: TexeraWebSocketRequest, uidOpt: Option[UInteger]): Unit = {
    wsInput.onNext((req, uidOpt))
  }

}
