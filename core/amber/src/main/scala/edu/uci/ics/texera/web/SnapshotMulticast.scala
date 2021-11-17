package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.common.client.AmberClient
import rx.lang.scala.{Observer, Subject, Subscription}

abstract class SnapshotMulticast[T] {
  private val subject = Subject[T]()

  // push updates to current subscribers
  def send(t: T): Unit = subject.onNext(t)

  /**
    * send snapshot to a newly-subscribed subscriber
    * in current thread. Then subscribe the observer
    * to push updates. This is NOT thread-safe if the
    * SnapshotMulticast instance subscribes events from
    * amber client.
    */
  def subscribe(observer: Observer[T]): Subscription = {
    sendSnapshotTo(observer)
    subject.onTerminateDetach.subscribe(observer)
  }

  /**
    * send snapshot to a newly-subscribed subscriber
    * in amber client to maintain a single-thread behavior.
    * Then subscribe the observer to push updates.
    */
  def subscribeWithAmberClient(observer: Observer[T], client: AmberClient): Subscription = {
    client.executeClosureSync {
      sendSnapshotTo(observer)
      subject.onTerminateDetach.subscribe(observer)
    }
  }

  /**
    * send "logical" snapshot to the observer.
    * The snapshot can be a sequence of objects.
    */
  def sendSnapshotTo(observer: Observer[T]): Unit

}
