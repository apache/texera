package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.withLock
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import rx.lang.scala.{Observable, Subject, Subscription}

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class StateStore[T](defaultState: T) {

  private var isModifying = false
  private implicit val lock: ReentrantLock = new ReentrantLock()

  private var currentState: T = defaultState
  private val stateChangedSubject = Subject[(T, T)].toSerialized
  private val syncableState = new SyncableState()

  def getStateThenConsume[X](next: T => X): X = {
    withLock {
      next(currentState)
    }
  }

  def updateState(func: T => T): Unit = {
    withLock {
      assert(!isModifying, "Cannot recursively update state or update state inside onChanged")
      isModifying = true
      val newState = func(currentState)
      stateChangedSubject.onNext((currentState, newState))
      isModifying = false
      currentState = newState
    }
  }

  def getObservable: Observable[(T, T)] = stateChangedSubject

  class SyncableState {
    stateChangedSubject.subscribe(diff => {
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
