package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.CommandCompleted

import scala.concurrent.ExecutionContext

abstract class LinkStrategy(
    val from: WorkerLayer,
    val to: WorkerLayer,
    val batchSize: Int,
    val inputNum: Int
) extends Serializable {

  val tag = LinkTag(from.tag, to.tag, inputNum)

  def getPolicies:Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])]
}
