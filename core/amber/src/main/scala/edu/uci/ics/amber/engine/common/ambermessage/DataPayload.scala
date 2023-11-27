package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

sealed trait DataPayload extends WorkflowFIFOMessagePayload {}

final case class EpochMarker(
    id: String,
    scope: PhysicalPlan,
    command: Option[ControlCommand[_]]
) extends DataPayload

final case class EndOfUpstream() extends DataPayload

final case class DataFrame(frame: Array[ITuple]) extends DataPayload {
  val inMemSize: Long = {
    frame.map(_.inMemSize).sum
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[DataFrame]) return false
    val other = obj.asInstanceOf[DataFrame]
    if (other eq null) return false
    if (frame.length != other.frame.length) {
      return false
    }
    var i = 0
    while (i < frame.length) {
      if (frame(i) != other.frame(i)) {
        return false
      }
      i += 1
    }
    true
  }
}
