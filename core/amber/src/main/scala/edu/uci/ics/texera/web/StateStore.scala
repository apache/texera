package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.withLock
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import io.reactivex.rxjava3.core.{Observable, Observer, Single}
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.{BehaviorSubject, PublishSubject}

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import java.util

class StateStore[T](defaultState: T) {

  private val stateSubject = BehaviorSubject.create[T]()
  private val serializedSubject = stateSubject.toSerialized
  private implicit val lock: ReentrantLock = new ReentrantLock()
  private val diffHandlers = new mutable.ArrayBuffer[(T, T) => Iterable[TexeraWebSocketEvent]]
  private val diffSubject = serializedSubject
    .startWith(Single.just(defaultState))
    .buffer(2)
    .map[Iterable[TexeraWebSocketEvent]] { states: util.List[T] =>
      withLock {
        diffHandlers.flatMap(f => f(states.get(0), states.get(1)))
      }
    }

  def getStateThenConsume[X](next: T => X): X = {
    next(stateSubject.getValue)
  }

  def updateState(func: T => T): Unit = {
    val newState = func(stateSubject.getValue)
    serializedSubject.onNext(newState)
  }

  def registerDiffHandler(handler: (T, T) => Iterable[TexeraWebSocketEvent]): Disposable = {
    withLock {
      diffHandlers.append(handler)
    }
    Disposable.fromAction { () =>
      withLock {
        diffHandlers -= handler
      }
    }
  }

  def getWebsocketEventObservable: Observable[Iterable[TexeraWebSocketEvent]] = diffSubject

  def getStateObservable: Observable[T] = serializedSubject

}
