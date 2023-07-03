package edu.uci.ics.amber.engine.architecture.pythonworker

import com.twitter.util.{Await, Promise}
import edu.uci.ics.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.{
  ControlElement,
  ControlElementV2,
  DataElement
}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
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
import scala.math.pow

class PythonProxyClient(portNumberPromise: Promise[Int], val actorId: ActorVirtualIdentity)
    extends Runnable
    with AmberLogging
    with AutoCloseable
    with WorkerBatchInternalQueue {

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-client", 0, Long.MaxValue)
  val location: Location = (() => {
    // Read port number from promise until it's available
    val portNumber = Await.result(portNumberPromise)
    Location.forGrpcInsecure("localhost", portNumber)
  })()

  private val MAX_TRY_COUNT: Int = 5
  private val UNIT_WAIT_TIME_MS = 200
  private var flightClient: FlightClient = _
  private var running: Boolean = true

  override def run(): Unit = {
    establishConnection()
    mainLoop()
  }

  def establishConnection(): Unit = {
    var connected = false
    var tryCount = 0
    while (!connected && tryCount <= MAX_TRY_COUNT) {
      try {
        flightClient = FlightClient.builder(allocator, location).build()
        connected = new String(flightClient.doAction(new Action("heartbeat")).next.getBody) == "ack"
        if (!connected)
          throw new RuntimeException("heartbeat failed")
      } catch {
        case _: RuntimeException =>
          val retryWaitTimeInMs = UNIT_WAIT_TIME_MS * pow(2, tryCount).toInt
          logger.warn(
            s"Failed to connect to Flight Server in this attempt, retrying after $retryWaitTimeInMs ms... remaining attempts: ${MAX_TRY_COUNT - tryCount}"
          )
          flightClient.close()
          Thread.sleep(retryWaitTimeInMs)
          tryCount += 1
      }
    }
    if (!connected) {
      throw new WorkflowRuntimeException(
        s"Failed to connect to Flight Server after $MAX_TRY_COUNT attempts. Abort!"
      )
    }
  }

  def mainLoop(): Unit = {
    while (running) {
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

  def sendControlV2(
      from: ActorVirtualIdentity,
      payload: ControlPayloadV2
  ): Result = {
    val controlMessage = PythonControlMessage(from, payload)
    val action: Action = new Action("control", controlMessage.toByteArray)

    logger.debug(s"sending control $controlMessage")
    // Arrow allows multiple results from the Action call return as a stream (interator).
    // In Arrow 11, it alerts if the results are not consumed fully.
    val results = flightClient.doAction(action)
    // As we do our own Async RPC management, we are currently not using results from Action call.
    // In the future, this results can include credits for flow control purpose.
    val result = results.next()

    // However, we will only expect exactly one result for now.
    assert(!results.hasNext)

    result
  }

  def sendControlV1(from: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    payload match {
      case controlInvocation: ControlInvocation =>
        val controlInvocationV2: ControlInvocationV2 = controlInvocationToV2(controlInvocation)
        sendControlV2(from, controlInvocationV2)
      case returnInvocation: ReturnInvocation =>
        val returnInvocationV2: ReturnInvocationV2 = returnInvocationToV2(returnInvocation)
        sendControlV2(from, returnInvocationV2)
    }
  }

  private def writeArrowStream(
      tuples: mutable.Queue[Tuple],
      from: ActorVirtualIdentity,
      isEnd: Boolean
  ): Unit = {

    val schema = if (tuples.isEmpty) new Schema() else tuples.front.getSchema
    val descriptor = FlightDescriptor.command(PythonDataHeader(from, isEnd).toByteArray)
    logger.info(
      s"sending data with descriptor ${PythonDataHeader(from, isEnd)}, schema $schema, size of batch ${tuples.size}"
    )
    val flightListener = new SyncPutListener
    val schemaRoot = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
    val writer = flightClient.startPut(descriptor, schemaRoot, flightListener)
    schemaRoot.allocateNew()
    while (tuples.nonEmpty) {
      ArrowUtils.appendTexeraTuple(tuples.dequeue(), schemaRoot)
    }
    writer.putNext()
    schemaRoot.clear()
    writer.completed()
    flightListener.getResult()
    flightListener.close()

  }

  override def close(): Unit = {

    val action: Action = new Action("shutdown")
    flightClient.doAction(action) // do not expect reply

    flightClient.close()

    // stop the main loop
    running = false
  }

}
