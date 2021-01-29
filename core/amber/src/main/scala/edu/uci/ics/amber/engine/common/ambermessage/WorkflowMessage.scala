package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

trait WorkflowMessage extends Serializable {
  val from: VirtualIdentity
  val sequenceNumber: Long
}
