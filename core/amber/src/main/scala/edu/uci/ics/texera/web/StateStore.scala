package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.withLock
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import io.reactivex.rxjava3.core.{ObservableSource, Single, SingleSource}
import io.reactivex.rxjava3.subjects.BehaviorSubject

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class StateStore[T](defaultState: T) {

  private val stateSubject = new BehaviorSubject[T]
  private val serializedSubject = stateSubject.toSerialized
  private val diffSubject = serializedSubject.startWith(Single.just(defaultState)).buffer(2)
  private val syncableState = new SyncableState()

  def getStateThenConsume[X](next: T => X): X = {
      next(stateSubject.getValue)
  }

  def updateState(func: T => T): Unit = {
      val newState = func(stateSubject.getValue)
      serializedSubject.onNext(newState)
  }

  def getObservable: Observable[(T, T)] = stateSubject

  class SyncableState {
    stateSubject.subscribe(diff => {
      val oldState = diff._1
      val newState = diff._2
      val events = onChangedHandlers.values.flatMap(handler => handler(oldState, newState))
      callbackSubject.onNext(events)
    })

    private var handlerId = 0
    private val onChangedHandlers = mutable.HashMap[Int, (T, T) => Iterable[TexeraWebSocketEvent]]()
    private val callbackSubject = Subject[Iterable[TexeraWebSocketEvent]].toSerialized

    def subscribe(callback: Iterable[TexeraWebSocketEvent] => Unit): Subscription = {
      withLock {
        val events =
          onChangedHandlers.values.flatMap(handler => handler(defaultState, currentState))
        callback(events)
      }
      callbackSubject.subscribe(callback)
    }

    def registerStateChangeHandler(
        handler: (T, T) => Iterable[TexeraWebSocketEvent]
    ): Subscription = {
      onChangedHandlers(handlerId) = handler
      val id = handlerId
      val sub = Subscription {
        onChangedHandlers.remove(id)
      }
      handlerId += 1
      sub
    }
  }

  def getSyncableState: SyncableState = syncableState

}
