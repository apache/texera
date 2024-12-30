package edu.uci.ics.amber.engine.common.ambermessage

import akka.actor.{ActorRef, Address}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

sealed trait RecoveryPayload extends Serializable {}

// Notify controller on worker recovery starts/ends
final case class UpdateRecoveryStatus(isRecovering: Boolean) extends RecoveryPayload

// Notify upstream worker to resend output to another worker for recovery
final case class ResendOutputTo(vid: ActorVirtualIdentity, ref: ActorRef) extends RecoveryPayload

// Notify controller when the machine fails and triggers recovery
final case class NotifyFailedNode(addr: Address) extends RecoveryPayload
