package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

trait WorkflowMessageGeneric[+T] extends Serializable {
  val from: VirtualIdentity
  val sequenceNumber: Long
  val payload: T
}

object WorkflowMessage {
  type WorkflowMessage = WorkflowMessageGeneric[_]
}
