package edu.uci.ics.amber.engine.architecture.scheduling

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef

import scala.collection.mutable.ArrayBuffer

class WorkflowScheduler(
    availableNodes: Array[Address],
    networkCommunicationActor: NetworkSenderActorRef,
    ctx: ActorContext
) {

  val regionsToSchedule: ArrayBuffer[PipelinedRegion] = new ArrayBuffer[PipelinedRegion]()
  val regionsCurrentlyScheduled: ArrayBuffer[PipelinedRegion] = new ArrayBuffer[PipelinedRegion]()

}
