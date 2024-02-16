package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

trait CheckpointSupport {
  def serializeState(
      currentIteratorState: Iterator[(ITuple, Option[PortIdentity])],
      checkpoint: CheckpointState
  ): Iterator[(ITuple, Option[PortIdentity])]

  def deserializeState(
      checkpoint: CheckpointState
  ): Iterator[(ITuple, Option[PortIdentity])]

  def getEstimatedCheckpointCost: Long

}
