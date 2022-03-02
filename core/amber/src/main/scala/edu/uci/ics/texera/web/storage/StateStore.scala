package edu.uci.ics.texera.web.storage

import edu.uci.ics.texera.Utils.withLock
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import io.reactivex.rxjava3.core.{Observable, Single}
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.BehaviorSubject

import java.util
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class StateStore[T](defaultState: T) {

  private val stateSubject = BehaviorSubject.createDefault(defaultState)
  private val serializedSubject = stateSubject.toSerialized
  private implicit val lock: ReentrantLock = new ReentrantLock()
  private val diffHandlers = new mutable.ArrayBuffer[(T, T) => Iterable[TexeraWebSocketEvent]]
  private val diffSubject = serializedSubject
    .startWith(Single.just(defaultState))
    .buffer(2, 1)
    .filter(states => states.get(0) != states.get(1))
    .map[Iterable[TexeraWebSocketEvent]] { states: util.List[T] =>
      withLock {
        diffHandlers.flatMap(f => f(states.get(0), states.get(1)))
      }
    }

  def getState: T = stateSubject.getValue

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

  def getWebsocketEventObservable: Observable[Iterable[TexeraWebSocketEvent]] =
    diffSubject.onTerminateDetach()

  def getStateObservable: Observable[T] = serializedSubject.onTerminateDetach()

}
