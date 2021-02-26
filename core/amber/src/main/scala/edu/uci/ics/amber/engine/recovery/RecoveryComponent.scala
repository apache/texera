package edu.uci.ics.amber.engine.recovery

import com.twitter.util.Promise

abstract class RecoveryComponent {

  def isRecovering: Boolean = !completion.isDefined

  def onComplete(callback: () => Unit): Unit = {
    completion.onSuccess(x => callback())
  }

  protected def setRecoveryCompleted(): Unit = completion.setValue(null)

  private val completion = new Promise[Void]()

}
