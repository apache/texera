package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

// TODO: remove redundant info
case class WorkerInfo(
    id: ActorVirtualIdentity,
    var state: WorkerState,
    var stats: WorkerStatistics,
    upstreamChannels: mutable.HashSet[ChannelID],
    @transient var ref: ActorRef
) extends Serializable
