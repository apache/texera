package edu.uci.ics.amber.engine.architecture.pythonworker

import com.google.common.primitives.Longs
import com.twitter.util.Promise
import edu.uci.ics.amber.core.marker.{EndOfInputChannel, StartOfInputChannel, State}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayloadV2.Value.{
  ControlInvocation => ControlInvocationV2,
  ReturnInvocation => ReturnInvocationV2
}
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.util.ArrowUtils
import edu.uci.ics.amber.virtualidentity.ActorVirtualIdentity
import org.apache.arrow.flight._
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables

import java.io.IOException
import java.net.ServerSocket
import java.nio.charset.Charset
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

private class AmberProducer(
    actorId: ActorVirtualIdentity,
    outputPort: NetworkOutputGateway,
    promise: Promise[Int]
) extends NoOpFlightProducer {
  var _portNumber: AtomicInteger = new AtomicInteger(0)

  def portNumber: AtomicInteger = _portNumber

  override def doAction(
      context: FlightProducer.CallContext,
      action: Action,
      listener: FlightProducer.StreamListener[Result]
  ): Unit = {
    action.getType match {
      case "control" =>
        val pythonControlMessage = PythonControlMessage.parseFrom(action.getBody)
        pythonControlMessage.payload.value match {
          case r: ReturnInvocationV2 =>
            outputPort.sendTo(
              to = pythonControlMessage.tag,
              payload = r.value
            )

          case c: ControlInvocationV2 =>
            outputPort.sendTo(
              to = pythonControlMessage.tag,
              payload = c.value
            )
          case payload =>
            throw new RuntimeException(s"not supported payload $payload")
        }

        // get little-endian representation of credits
        var creditVal: Long = 30L // TODO : replace with actual credit value
        val creditByteArr: Array[Byte] =
          ByteBuffer.allocate(Longs.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(creditVal).array

        listener.onNext(
          new Result(creditByteArr)
        )
        listener.onCompleted()
      case "handshake" =>
        val strPortNumber: String = new String(action.getBody, Charset.forName("UTF-8"))
        // Receive the port number from Python and put it into promise
        promise.setValue(strPortNumber.toInt)
        listener.onNext(new Result("ok".getBytes))
        listener.onCompleted()
      case _ => throw new NotImplementedError()
    }

  }

  override def acceptPut(
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]
  ): Runnable = { () =>
    val dataHeader: PythonDataHeader = PythonDataHeader
      .parseFrom(flightStream.getDescriptor.getCommand)
    val to: ActorVirtualIdentity = dataHeader.tag
    val root = flightStream.getRoot

    // send back ack with credits on ackStream
    val bufferAllocator = new RootAllocator(8 * 1024)
    try {
      val arrowBuf: ArrowBuf = bufferAllocator.buffer(Longs.BYTES + 4)
      arrowBuf.writeLong(
        31L
      ) // TODO : replace with actual credit value
      ackStream.onNext(PutResult.metadata(arrowBuf))
      arrowBuf.close()
    } finally if (bufferAllocator != null) bufferAllocator.close()

    // consume all data in the stream, it will store on the root vectors.
    while (flightStream.next) {}

    // closing the stream will release the dictionaries
    flightStream.takeDictionaryOwnership

    dataHeader.payloadType match {
      case "StartOfInputChannel" =>
        assert(root.getRowCount == 0)
        outputPort.sendTo(to, MarkerFrame(StartOfInputChannel()))
      case "EndOfInputChannel" =>
        assert(root.getRowCount == 0)
        outputPort.sendTo(to, MarkerFrame(EndOfInputChannel()))
      case "State" =>
        assert(root.getRowCount == 1)
        outputPort.sendTo(to, MarkerFrame(State(Some(ArrowUtils.getTexeraTuple(0, root)))))
      case _ => // normal data batches
        val queue = mutable.Queue[Tuple]()
        for (i <- 0 until root.getRowCount)
          queue.enqueue(ArrowUtils.getTexeraTuple(i, root))
        outputPort.sendTo(to, DataFrame(queue.toArray))
    }
  }
}

class PythonProxyServer(
    outputPort: NetworkOutputGateway,
    val actorId: ActorVirtualIdentity,
    promise: Promise[Int]
) extends Runnable
    with AutoCloseable
    with AmberLogging {
  private lazy val portNumber: AtomicInteger = new AtomicInteger(getFreeLocalPort)

  def getPortNumber: AtomicInteger = portNumber

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue)

  val producer: FlightProducer = new AmberProducer(actorId, outputPort, promise)

  val location: Location = (() => {
    Location.forGrpcInsecure("localhost", portNumber.intValue())
  })()

  val server: FlightServer = FlightServer.builder(allocator, location, producer).build()

  override def run(): Unit = {
    server.start()
  }

  @throws[Exception]
  override def close(): Unit = {
    AutoCloseables.close(server, allocator)
  }

  /**
    * Get a random free port.
    *
    * @return The port number.
    * @throws IOException , might happen when getting a free port.
    */
  @throws[IOException]
  private def getFreeLocalPort: Int = {
    var s: ServerSocket = null
    try {
      // ServerSocket(0) results in availability of a free random port
      s = new ServerSocket(0)
      s.getLocalPort
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally {
      assert(s != null)
      s.close()
    }
  }
}
