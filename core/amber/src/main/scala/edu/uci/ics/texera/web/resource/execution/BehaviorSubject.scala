package edu.uci.ics.texera.web.resource.execution

import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.{Observer, Subject, Subscription}

abstract class BehaviorSubject extends Subject[TexeraWebSocketEvent] {
  def sendSnapshotTo(observer: Observer[TexeraWebSocketEvent]):Unit
  override def subscribe(observer: Observer[TexeraWebSocketEvent]): Subscription = {
    // sync with subscriber before streaming data
    sendSnapshotTo(observer)
    super.subscribe(observer)
  }
}
