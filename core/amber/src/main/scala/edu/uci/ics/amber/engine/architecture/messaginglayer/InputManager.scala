package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import edu.uci.ics.amber.engine.architecture.worker.managers.InputPortMaterializationReaderThread

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable

class InputManager(val actorId: ActorVirtualIdentity) extends AmberLogging {
  private var inputBatch: Array[Tuple] = _
  private var currentInputIdx: Int = -1
  var currentChannelId: ChannelIdentity = _
  var inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement] = _

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  private val inputPortMaterializationReaderThreads
      : mutable.HashMap[PortIdentity, List[InputPortMaterializationReaderThread]] =
    mutable.HashMap()

  def getAllPorts: Set[PortIdentity] = {
    this.ports.keys.toSet
  }

  def addPort(portId: PortIdentity, schema: Schema, urisToRead: List[URI]): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort(schema)

    // if a materialization URI is provided, set up a materialization reader thread
    setupInputPortMaterializationReaderThreads(portId, urisToRead)
  }

  private def setupInputPortMaterializationReaderThreads(
      portId: PortIdentity,
      uris: List[URI]
  ): Unit = {
    val readerThreads = uris.map { uri =>
      {
        new InputPortMaterializationReaderThread(
          uri = uri,
          inputMessageQueue = this.inputMessageQueue,
          workerActorId = this.actorId
        )
      }
    }

    inputPortMaterializationReaderThreads(portId) = readerThreads
  }

  def getInputPortReaderThreads: Map[PortIdentity, List[InputPortMaterializationReaderThread]] = {
    this.inputPortMaterializationReaderThreads.toMap
  }

  def startInputPortReaderThreads(): Unit = {
    this.inputPortMaterializationReaderThreads.values.foreach(threadList =>
      threadList.foreach(readerThread => {
        readerThread.start()
      })
    )
  }

  def getPort(portId: PortIdentity): WorkerPort = ports(portId)

  def isPortCompleted(portId: PortIdentity): Boolean = {
    // a port without channels is not completed.
    if (this.ports(portId).channels.isEmpty) {
      return false
    }
    this.ports(portId).channels.values.forall(completed => completed)
  }

  def hasUnfinishedInput: Boolean = inputBatch != null && currentInputIdx + 1 < inputBatch.length

  def getNextTuple: Tuple = {
    currentInputIdx += 1
    inputBatch(currentInputIdx)
  }

  def getCurrentTuple: Tuple = {
    if (inputBatch == null) {
      null
    } else if (inputBatch.isEmpty) {
      null // TODO: create input exhausted
    } else {
      inputBatch(currentInputIdx)
    }
  }

  def initBatch(channelId: ChannelIdentity, batch: Array[Tuple]): Unit = {
    currentChannelId = channelId
    inputBatch = batch
    currentInputIdx = -1
  }
}
