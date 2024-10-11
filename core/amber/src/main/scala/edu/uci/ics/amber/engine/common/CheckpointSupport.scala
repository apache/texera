package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.model.tuple.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

trait CheckpointSupport {
  def serializeState(
      currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])],
      checkpoint: CheckpointState
  ): Iterator[(TupleLike, Option[PortIdentity])]

  def deserializeState(
      checkpoint: CheckpointState
  ): Iterator[(TupleLike, Option[PortIdentity])]

  def getEstimatedCheckpointCost: Long

}
