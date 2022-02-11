package edu.uci.ics.texera.web

import io.reactivex.rxjava3.disposables.Disposable

import scala.collection.mutable

trait SubscriptionManager {

  private val subscriptions = mutable.ArrayBuffer[Disposable]()

  def addSubscription(sub: Disposable): Unit = {
    subscriptions.append(sub)
  }

  def unsubscribeAll(): Unit = {
    subscriptions.foreach(_.dispose())
    subscriptions.clear()
  }

}
