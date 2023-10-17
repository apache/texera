package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class PauseManager(dataProcessor: DataProcessor) {

  private val globalPauses = new mutable.HashSet[PauseType]()
  private val specificInputPauses =
    new mutable.HashMap[PauseType, mutable.Set[ActorVirtualIdentity]]
      with mutable.MultiMap[PauseType, ActorVirtualIdentity]

  def pause(pauseType: PauseType): Unit = {
    globalPauses.add(pauseType)
    // disable all data queues
    dataProcessor.internalQueue.dataQueues.values.foreach(q => q.enable(false))
  }

  def pauseInputChannel(pauseType: PauseType, inputs: List[ActorVirtualIdentity]): Unit = {
    inputs.foreach(input => {
      specificInputPauses.addBinding(pauseType, input)
      // disable specified data queues
      dataProcessor.internalQueue.dataQueues(input.name).enable(false)
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
      dataProcessor.internalQueue.dataQueues.values.foreach(q => q.enable(true))
      return
    }
    // need to resume specific input channels
    val pausedChannels = specificInputPauses.values.flatten.map(id => id.name).toSet
    dataProcessor.internalQueue.dataQueues.foreach(kv => {
      if (!pausedChannels.contains(kv._1)) {
        kv._2.enable(true)
      }
    })
  }

  def isPaused: Boolean = {
    globalPauses.nonEmpty
  }

}
