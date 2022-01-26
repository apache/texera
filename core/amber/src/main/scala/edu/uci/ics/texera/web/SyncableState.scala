package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.withLock
import rx.lang.scala.Subscription

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class SyncableState[T](defaultStateGen: => T) {

  private var state: T = defaultStateGen
  private var isModifying = false
  private var callbackId = 0
  private val onChangedCallbacks = mutable.HashMap[Int, (T, T) => Unit]()
  private implicit val lock: ReentrantLock = new ReentrantLock()

  def getStateThenConsume[X](next: T => X): X = {
    withLock {
      next(state)
    }
  }

  def acquireLock(): Unit = lock.lock()
  def releaseLock(): Unit = lock.unlock()

  def updateState(func: T => T): Unit = {
    withLock {
      assert(!isModifying, "Cannot recursively update state or update state inside onChanged")
      isModifying = true
      val newState = func(state)
      onChangedCallbacks.values.foreach(callback => callback(state, newState))
      isModifying = false
      state = newState
    }
  }

  def onChanged(callback: (T, T) => Unit): Subscription = {
    withLock {
      onChangedCallbacks(callbackId) = callback
      val id = callbackId
      val sub = Subscription {
        onChangedCallbacks.remove(id)
      }
      callbackId += 1
      sub
    }
  }

  def sendSnapshot(): Unit = {
    withLock {
      onChangedCallbacks.values.foreach(callback => callback(defaultStateGen, state))
    }
  }

}
