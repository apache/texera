package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{WorkflowDataMessage, WorkflowMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * We implement credit-based flow control. Suppose a sender worker S sends data in batches to a receiving worker R
  * using the network communicator actor NC. The different parts of flow control work as follows:
  *
  * 1. A worker has a fixed amount of credits for each of its sender workers. When R is expensive, its internal queue
  * starts getting filled with data from S. This leads to a decrease in credits available for S.
  *
  * 2. R sends the credits available in the NetworkAck() being sent to S. This includes acks for data messages and
  * control messages. The responsibility to decrease data sending lies on S now.
  *
  * 3. Upon receiving NetworkAck(), S saves the credit information and then does two things:
  * a) Tell parent to enable/disable backpressure: If the `buffer in NC` + `credit available in R` is less
  * than the `backlog of data in NC`, backpressure needs to be enabled. For this, the NC sends a control message
  * to S to enable backpressure (pause data processing) and adds R to `overloaded` list. On the other hand, if
  * `buffer in NC` + `credit available in R` is enough, then R is removed from the overloaded list (if present). If
  * the `overloaded` list is empty, then NC sends a request to S to disable backpressure (resume processing).
  *
  * b) It looks at its backlog and sends an amount of data, less than credits available, to congestion control.
  *
  * 3. If R sends a credit of 0, then S won't send any data as a response to NetworkAck(). This will lead to a problem
  * because then there is no way for S to know when the data in its congestion control module can be sent. Thus,
  * whenever S receives a credit of 0, it registers a periodic callback that serves as a trigger for it to send
  * credit poll request to R. Then, R responds with a NetworkAck() for the credits.
  *
  * 4. In our current design, the term "Credit" refers to the message in memory size in bytes.
  */
class FlowControl {
  val receiverCreditsMapping = new mutable.HashMap[ActorVirtualIdentity, Long]()
  var backpressureRequestSentToMainActor = false
  var receiverToCreditPollingHandle = new mutable.HashMap[ActorVirtualIdentity, Cancellable]()
  private val receiverStashedDataMessageMapping =
    new mutable.HashMap[ActorVirtualIdentity, mutable.Queue[WorkflowMessage]]()

  def getOverloadedReceivers(): ArrayBuffer[ActorVirtualIdentity] = {
    val overloadedReceivers = new ArrayBuffer[ActorVirtualIdentity]()
    receiverStashedDataMessageMapping.keys.foreach(receiverId => {
      if (
        receiverStashedDataMessageMapping(
          receiverId
        ).map(getInMemSize)
        .sum > Constants.localSendingBufferLimitPerReceiver + receiverCreditsMapping(receiverId)
      ) {
        overloadedReceivers.append(receiverId)
      }
    })
    overloadedReceivers
  }

  def stashData(receiverId: ActorVirtualIdentity, msg: WorkflowMessage): Unit = {
    hasStashedDataMessage(receiverId)
    receiverStashedDataMessageMapping(receiverId).enqueue(msg)

  }

  def getMessagesToForward(receiverId: ActorVirtualIdentity): Array[WorkflowMessage] = {
    val messagesToSend = new ArrayBuffer[WorkflowMessage]()

    initializeCreditIfNotExist(receiverId)
    breakable {
      while (hasStashedDataMessage(receiverId)) {
        val msg = receiverStashedDataMessageMapping(receiverId).front
        val credit = getInMemSize(msg)
        if (credit <= receiverCreditsMapping(receiverId)) {
          receiverStashedDataMessageMapping(receiverId).dequeue()
          messagesToSend.append(msg)
          decreaseCredit(receiverId, credit)
        } else {
          break
        }
      }
    }

    messagesToSend.toArray

  }

  /**
    * Decides whether parent should be backpressured based on the current data message put into
    * `receiverStashedDataMessageMapping` queue.
    */
  def shouldBackpressureParent(receiverId: ActorVirtualIdentity): Boolean = {
    Constants.flowControlEnabled &&
    receiverStashedDataMessageMapping
      .getOrElseUpdate(receiverId, new mutable.Queue[WorkflowMessage]()).map(getInMemSize)
      .sum > Constants.localSendingBufferLimitPerReceiver + receiverCreditsMapping.getOrElseUpdate(
      receiverId,
      Constants.unprocessedBatchesSizeLimitInBytesPerWorkerPair
    )
  }

  def updateCredits(receiverId: ActorVirtualIdentity, credits: Long): Unit = {
    if (credits <= 0) {
      receiverCreditsMapping(receiverId) = 0
    } else {
      receiverCreditsMapping(receiverId) = credits
    }
  }

  def decreaseCredit(receiverId: ActorVirtualIdentity, credit: Long): Unit = {
    receiverCreditsMapping(receiverId) = receiverCreditsMapping(receiverId) - credit
  }

  def hasStashedDataMessage(receiverId: ActorVirtualIdentity): Boolean = {
    receiverStashedDataMessageMapping
      .getOrElseUpdate(receiverId, new mutable.Queue[WorkflowMessage]())
      .nonEmpty
  }

  def initializeCreditIfNotExist(receiverId: ActorVirtualIdentity): Unit = {
    receiverCreditsMapping.getOrElseUpdate(
      receiverId,
      Constants.unprocessedBatchesSizeLimitInBytesPerWorkerPair
    )
  }
}
