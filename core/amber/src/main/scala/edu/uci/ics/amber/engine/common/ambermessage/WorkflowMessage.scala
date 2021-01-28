package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity

trait WorkflowMessage extends Serializable {
  val from: VirtualIdentity
  val sequenceNumber: Long
}
