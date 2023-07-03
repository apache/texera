package edu.uci.ics.amber.engine.architecture.pythonworker

import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputPort
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.InvocationConvertUtils.{
  controlInvocationToV1,
  returnInvocationToV1
}
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import com.twitter.util.Promise

import java.nio.charset.Charset

private class AmberProducer(
    controlOutputPort: NetworkOutputPort[ControlPayload],
    dataOutputPort: NetworkOutputPort[DataPayload],
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
        pythonControlMessage.payload match {
          case returnInvocation: ReturnInvocationV2 =>
            controlOutputPort.sendTo(
              to = pythonControlMessage.tag,
              payload = returnInvocationToV1(returnInvocation)
            )

          case controlInvocation: ControlInvocationV2 =>
            controlOutputPort.sendTo(
              to = pythonControlMessage.tag,
              payload = controlInvocationToV1(controlInvocation)
            )
          case payload =>
            throw new RuntimeException(s"not supported payload $payload")
        }
        listener.onNext(new Result("ack".getBytes))
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
    val isEnd: Boolean = dataHeader.isEnd

    val root = flightStream.getRoot

    // consume all data in the stream, it will store on the root vectors.
    while (flightStream.next) {
      ackStream.onNext(PutResult.metadata(flightStream.getLatestMetadata))
    }
    // closing the stream will release the dictionaries
    flightStream.takeDictionaryOwnership

    if (isEnd) {
      // EndOfUpstream
      assert(root.getRowCount == 0)
      dataOutputPort.sendTo(to, EndOfUpstream())
    } else {
      // normal data batches
      val queue = mutable.Queue[Tuple]()
      for (i <- 0 until root.getRowCount)
        queue.enqueue(ArrowUtils.getTexeraTuple(i, root))
      dataOutputPort.sendTo(to, DataFrame(queue.toArray))

    }

  }

}

class PythonProxyServer(
    controlOutputPort: NetworkOutputPort[ControlPayload],
    dataOutputPort: NetworkOutputPort[DataPayload],
    val actorId: ActorVirtualIdentity,
    promise: Promise[Int]
) extends Runnable
    with AutoCloseable
    with AmberLogging {
  private lazy val portNumber: AtomicInteger = new AtomicInteger(getFreeLocalPort)
  def getPortNumber: AtomicInteger = portNumber

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue);

  val producer: FlightProducer = new AmberProducer(controlOutputPort, dataOutputPort, promise)

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
    * @throws IOException  , might happen when getting a free port.
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
