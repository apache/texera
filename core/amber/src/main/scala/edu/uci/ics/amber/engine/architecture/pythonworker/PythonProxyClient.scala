package edu.uci.ics.amber.engine.architecture.pythonworker

import edu.uci.ics.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.{
  ControlElement,
  ControlElementV2,
  DataElement
}
import edu.uci.ics.amber.engine.common.ambermessage.InvocationConvertUtils.{
  controlInvocationToV2,
  returnInvocationToV2
}
import edu.uci.ics.amber.engine.common.ambermessage.{PythonControlMessage, _}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot

import scala.collection.mutable

class PythonProxyClient(portNumber: Int)
    extends Runnable
    with AutoCloseable
    with WorkerBatchInternalQueue {

  final val CHUNK_SIZE: Int = 100 // TODO: remove it
  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-client", 0, Long.MaxValue)
  val location: Location = Location.forGrpcInsecure("localhost", portNumber)

  private val MAX_TRY_COUNT: Int = 3
  private val WAIT_TIME_MS = 500
  private var flightClient: FlightClient = _

  override def run(): Unit = {
    establishConnection()
    mainLoop()
  }

  def establishConnection(): Unit = {
    var connected = false
    var tryCount = 0
    while (!connected && tryCount < MAX_TRY_COUNT) {
      try {
        println("trying to connect to " + location)
        flightClient = FlightClient.builder(allocator, location).build()
        connected = new String(flightClient.doAction(new Action("heartbeat")).next.getBody) == "ack"
        if (!connected) Thread.sleep(WAIT_TIME_MS)
      } catch {
        case _: FlightRuntimeException =>
          println("Flight CLIENT:\tNot connected to the server in this try.")
          flightClient.close()
          Thread.sleep(WAIT_TIME_MS)
          tryCount += 1
      }
      if (tryCount == MAX_TRY_COUNT)
        throw new RuntimeException(
          "Exceeded try limit of " + MAX_TRY_COUNT + " when connecting to Flight Server!"
        )
    }
  }

  def mainLoop(): Unit = {
    while (true) {
      // TODO: change to termination condition?
      getElement match {
        case DataElement(dataPayload, from) =>
          sendData(dataPayload, from)
        case ControlElement(cmd, from) =>
          sendControlV1(from, cmd)
        case ControlElementV2(cmd, from) =>
          sendControlV2(from, cmd)
      }
    }
  }

  def sendData(dataPayload: DataPayload, from: ActorVirtualIdentity): Unit = {
    dataPayload match {
      case DataFrame(frame) =>
        val tuples: mutable.Queue[Tuple] =
          mutable.Queue(frame.map(_.asInstanceOf[Tuple]): _*)
        writeArrowStream(tuples, from, isEnd = false)
      case EndOfUpstream() =>
        writeArrowStream(mutable.Queue(), from, isEnd = true)
    }
  }

  private def writeArrowStream(
      tuples: mutable.Queue[Tuple],
      from: ActorVirtualIdentity,
      isEnd: Boolean
  ): Unit = {
    val schema = if (tuples.isEmpty) new Schema() else tuples.front.getSchema
    val descriptor = FlightDescriptor.command(PythonDataHeader(from, isEnd).toByteArray)
    val flightListener = new SyncPutListener
    val schemaRoot = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
    val writer = flightClient.startPut(descriptor, schemaRoot, flightListener)

    while (tuples.nonEmpty) {
      writeChunk(tuples, schemaRoot, writer)
    }
    writer.completed()
    flightListener.getResult()
    flightListener.close()
  }

  private def writeChunk(
      tuples: mutable.Queue[Tuple],
      schemaRoot: VectorSchemaRoot,
      writer: FlightClient.ClientStreamListener
  ): Unit = {
    schemaRoot.allocateNew()
    while (schemaRoot.getRowCount < CHUNK_SIZE && tuples.nonEmpty)
      ArrowUtils.appendTexeraTuple(tuples.dequeue(), schemaRoot)
    writer.putNext()
    schemaRoot.clear()
  }

  def sendControlV1(from: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    payload match {
      case controlInvocation: ControlInvocation =>
        val controlInvocationV2: ControlInvocationV2 = controlInvocationToV2(controlInvocation)
        sendControlV2(from, controlInvocationV2)
      case returnInvocation: ReturnInvocation =>
        val returnInvocationV2: ReturnInvocationV2 = returnInvocationToV2(returnInvocation)
        // Let python handle -1
        if (returnInvocationV2.originalCommandId != -1) {
          sendControlV2(from, returnInvocationV2)
        }
    }
  }

  def sendControlV2(
      from: ActorVirtualIdentity,
      payload: ControlPayloadV2
  ): Result = {
    val controlMessage = PythonControlMessage(from, payload)
    val action: Action = new Action("control", controlMessage.toByteArray)
    flightClient.doAction(action).next()
  }

  override def close(): Unit = {
    val action: Action = new Action("shutdown")
    flightClient.doAction(action) // do not expect reply

    flightClient.close()
  }

}
