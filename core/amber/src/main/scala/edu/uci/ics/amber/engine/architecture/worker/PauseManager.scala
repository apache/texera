package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.InputGateway
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class PauseManager(val actorId: ActorVirtualIdentity, inputGateway: InputGateway)
    extends AmberLogging {

  private val globalPauses = new mutable.HashSet[PauseType]()
  private val specificInputPauses =
    new mutable.HashMap[PauseType, mutable.Set[ActorVirtualIdentity]]
      with mutable.MultiMap[PauseType, ActorVirtualIdentity]

  def pause(pauseType: PauseType): Unit = {
    globalPauses.add(pauseType)
    // disable all data queues
    inputGateway.getAllDataChannels.foreach(_.enable(false))
  }

  def pauseInputChannel(pauseType: PauseType, inputs: List[ActorVirtualIdentity]): Unit = {
    inputs.foreach(input => {
      specificInputPauses.addBinding(pauseType, input)
      // disable specified data queues
      inputGateway.getChannel(ChannelID(actorId, input, isControl = false)).enable(false)
    })
  }

  def resume(pauseType: PauseType): Unit = {
    globalPauses.remove(pauseType)
    specificInputPauses.remove(pauseType)

    // still globally paused no action, don't need to resume anything
    if (globalPauses.nonEmpty) {
      return
    }
    // global pause is empty, specific input pause is also empty, resume all
    if (specificInputPauses.isEmpty) {
      inputGateway.getAllDataChannels.foreach(_.enable(true))
      return
    }
    // need to resume specific input channels
    val pausedActorVids = specificInputPauses.values.flatten.toSet
    inputGateway.getAllDataChannels.foreach(_.enable(true))
    pausedActorVids.foreach { vid =>
      inputGateway.getChannel(ChannelID(actorId, vid, isControl = false)).enable(false)
    }
  }

  def isPaused: Boolean = {
    globalPauses.nonEmpty
  }

}
