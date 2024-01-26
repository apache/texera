package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

import scala.collection.mutable

// TODO: remove redundant info
case class WorkerInfo(
    id: ActorVirtualIdentity,
    var state: WorkerState,
    var stats: WorkerStatistics,
    upstreamChannels: mutable.HashSet[ChannelIdentity]
) extends Serializable
