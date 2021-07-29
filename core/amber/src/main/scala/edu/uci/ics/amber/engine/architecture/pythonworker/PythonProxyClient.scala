package edu.uci.ics.amber.engine.architecture.pythonworker

import edu.uci.ics.amber.engine.architecture.pythonworker.PythonProxyClient.communicate
import edu.uci.ics.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.{
  ControlElement,
  ControlElementV2,
  DataElement
}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambermessage.InvocationConvertUtils.controlInvocationToV2
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.pythonUDF.ArrowUtils
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets
import scala.collection.mutable

object MSG extends Enumeration {
  type MSGType = Value
  val HEALTH_CHECK: Value = Value
}

object PythonProxyClient {

  private def communicate(client: FlightClient, message: String): Array[Byte] =
    client.doAction(new Action(message)).next.getBody
}

case class PythonProxyClient(portNumber: Int, operator: IOperatorExecutor)
    extends Runnable
    with AutoCloseable
    with WorkerBatchInternalQueue {

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue);
  val location: Location = Location.forGrpcInsecure("localhost", portNumber)

  private val MAX_TRY_COUNT: Int = 3
  private val WAIT_TIME_MS = 500
  var schemaRoot: VectorSchemaRoot = _
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
        connected =
          new String(communicate(flightClient, "heartbeat"), StandardCharsets.UTF_8) == "ack"
        if (!connected) Thread.sleep(WAIT_TIME_MS)
      } catch {
        case e: FlightRuntimeException =>
          System.out.println("Flight CLIENT:\tNot connected to the server in this try.")
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

      getElement match {
        case DataElement(dataPayload, from) =>
          dataPayload match {
            case DataFrame(frame) =>
              val tuples = mutable.Queue(frame.map((t: ITuple) => t.asInstanceOf[Tuple]): _*)
              writeArrowStream(flightClient, tuples, 100, from, false)
            case EndOfUpstream() =>
              val q = mutable.Queue(
                Tuple
                  .newBuilder(
                    edu.uci.ics.texera.workflow.common.tuple.schema.Schema.newBuilder().build()
                  )
                  .build()
              )
              writeArrowStream(flightClient, q, 100, from, true)
          }
        case ControlElement(cmd, from) =>
          sendControl(cmd, from)
        case ControlElementV2(cmd, from) =>
          sendControl(cmd.asInstanceOf[ControlInvocationV2], from)

      }
    }

  }

  def sendControl(cmd: ControlPayload, from: ActorVirtualIdentity): Unit = {
    cmd match {
      case controlInvocation: ControlInvocation =>
        val controlInvocationV2: ControlInvocationV2 = controlInvocationToV2(controlInvocation)
        send(from, controlInvocationV2)
      case ReturnInvocation(originalCommandID, _) =>
        println("JAVA receive return payload " + originalCommandID)
    }
  }

  def sendControl(cmd: ControlInvocationV2, from: ActorVirtualIdentity): Unit =
    send(from, cmd)

  def send(
      from: ActorVirtualIdentity,
      cmd: ControlInvocationV2
  ): Result = {

    val controlMessage = toPythonControlMessage(from, cmd)
    println("JAVA sending " + controlMessage.toString)
    val action: Action = new Action("control", controlMessage.toByteArray)
    flightClient.doAction(action).next()
  }

  def toPythonControlMessage(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayloadV2
  ): PythonControlMessage = {
    PythonControlMessage(
      tag = from,
      payload = controlPayload
    )
  }

  override def close(): Unit = {

    val action: Action = new Action("shutdown", "".getBytes)
    flightClient.doAction(action) // do not expect reply
    flightClient.close()
  }

  private def writeArrowStream(
      client: FlightClient,
      values: mutable.Queue[Tuple],
      chunkSize: Int = 100,
      from: ActorVirtualIdentity,
      isEnd: Boolean
  ): Unit = {
    if (values.nonEmpty) {
      val cachedTuple = values.front
      val schema = cachedTuple.getSchema
      val arrowSchema = ArrowUtils.fromTexeraSchema(schema)
      val flightListener = new SyncPutListener

      val schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)

      val writer =
        client.startPut(
          FlightDescriptor.command(PythonDataHeader(from, isEnd).toByteArray),
          schemaRoot,
          flightListener
        )

      try {
        while (values.nonEmpty) {
          schemaRoot.allocateNew()
          while (schemaRoot.getRowCount < chunkSize && values.nonEmpty)
            ArrowUtils.appendTexeraTuple(values.dequeue(), schemaRoot)
          writer.putNext()
          schemaRoot.clear()
        }
        writer.completed()
        flightListener.getResult()
        flightListener.close()
        schemaRoot.clear()

      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

  }

}
