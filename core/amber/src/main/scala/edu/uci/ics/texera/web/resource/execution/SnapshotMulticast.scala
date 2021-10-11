package edu.uci.ics.texera.web.resource.execution

import edu.uci.ics.amber.engine.common.AmberClient
import rx.lang.scala.{Observable, Observer, Subject, Subscription}

object SnapshotMulticast{
  def subscribeSync[T](snapshotSubject: SnapshotMulticast[T], observer: Observer[T], client: AmberClient): Subscription ={
    client.executeClosureSync{
      snapshotSubject.sendSnapshotTo(observer)
      snapshotSubject.subscribe(observer)
    }
  }
}

abstract class SnapshotMulticast[T] {
  private val subject = Subject[T]()

  def send(t:T): Unit = subject.onNext(t)

  def subscribe(observer: Observer[T]): Subscription = subject.subscribe(observer)

  def sendSnapshotTo(observer:Observer[T]):Unit

}
