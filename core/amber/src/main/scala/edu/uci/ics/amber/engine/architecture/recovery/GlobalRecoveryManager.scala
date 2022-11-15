package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class GlobalRecoveryManager(onRecoveryStart: () => Unit, onRecoveryEnd: () => Unit) {
  private val recovering = mutable.HashSet[ActorVirtualIdentity]()
  def markRecoveryStatus(vid: ActorVirtualIdentity, isRecovering: Boolean): Unit = {
    val globalRecovering = recovering.nonEmpty
    if (isRecovering) {
      recovering.add(vid)
    } else {
      recovering.remove(vid)
    }
    if (!globalRecovering && recovering.nonEmpty) {
      onRecoveryStart()
    }
    if (globalRecovering && recovering.isEmpty) {
      onRecoveryEnd()
    }
  }
}
