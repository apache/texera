package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager
import edu.uci.ics.amber.engine.common.ambermessage.EpochMarker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class EpochManager(
    dp: DataProcessor,
    outputManager: OutputManager,
    asyncRPCServer: AsyncRPCServer
) {

  private val epochMarkerReceived =
    new mutable.HashMap[String, Set[ActorVirtualIdentity]]().withDefaultValue(Set())

  // markers the arrival of an epoch marker,
  // returns a boolean indicating if the epoch marker is completely received from all senders within scope
  def processEpochMarker(from: ActorVirtualIdentity, marker: EpochMarker): Unit = {
    val markerId = marker.id
    dp.pauseManager.pauseInputChannel(EpochMarkerPause(markerId), List(from))
    epochMarkerReceived.update(markerId, epochMarkerReceived(markerId) + from)

    // check if the epoch marker is completed
    val sendersWithinScope = dp.upstreamLinkStatus.allUncompletedSenders.filter(sender =>
      marker.scope.links.contains(dp.upstreamLinkStatus.upstreamMapReverse(sender))
    )
    val epochMarkerCompleted = epochMarkerReceived(markerId) == sendersWithinScope
    if (epochMarkerCompleted) {
      epochMarkerReceived.remove(markerId) // clean up on epoch marker completion
      triggerEpochMarkerOnCompletion(marker)
    }

  }

  def triggerEpochMarkerOnCompletion(marker: EpochMarker): Unit = {
    // invoke the control command carried with the epoch marker
    if (marker.command.nonEmpty) {
      this.asyncRPCServer.receive(
        ControlInvocation(AsyncRPCClient.IgnoreReply, marker.command.get),
        asyncRPCServer.actorId
      )
    }
    // if this operator is not the final destination of the marker, pass it downstream
    if (!marker.scope.sinkOperators.contains(dp.opExecConfig.id)) {
      this.outputManager.emitEpochMarker(marker)
    }
    // unblock input channels
    dp.pauseManager.resume(EpochMarkerPause(marker.id))
  }

}
