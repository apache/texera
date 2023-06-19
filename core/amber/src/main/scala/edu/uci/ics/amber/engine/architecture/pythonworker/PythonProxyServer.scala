package edu.uci.ics.amber.engine.architecture.pythonworker

import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputPort
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.InvocationConvertUtils.{controlInvocationToV1, returnInvocationToV1}
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables

import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source

private class AmberProducer(
    controlOutputPort: NetworkOutputPort[ControlPayload],
    dataOutputPort: NetworkOutputPort[DataPayload]
) extends NoOpFlightProducer {

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
                         portNumber: Int,
    controlOutputPort: NetworkOutputPort[ControlPayload],
    dataOutputPort: NetworkOutputPort[DataPayload],
    val actorId: ActorVirtualIdentity
) extends Runnable
    with AutoCloseable
    with AmberLogging {

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue);
//  val location: Location = Location.forGrpcInsecure("localhost", portNumber)
  private val UNIT_WAIT_TIME_MS = 200
  val location: Location = (() => {
//    val environVariableName = "WORKER_PYTHON" + actorId.name.charAt(actorId.name.length - 1) + "_INPUT_PORT"
//    var portNumber = System.getenv(environVariableName)
//    if (portNumber == null) {
//      while (portNumber == null) {
//        Thread.sleep(UNIT_WAIT_TIME_MS)
//        logger.info(s"Waiting for input port for: $environVariableName")
//        portNumber = System.getenv(environVariableName + "_INPUT_PORT")
//      }
//      logger.info(s"Input port found for: $environVariableName at $portNumber")
//    }
//    Location.forGrpcInsecure("localhost", portNumber.toInt)
//    val fileName = "connection" + actorId.name + "_input.info"
//
//    val filePath = Paths.get("").toAbsolutePath.resolve(fileName).toAbsolutePath
//    logger.info(s"will try input file at: $filePath")
//
//    if (!Files.exists(filePath)) {
//      logger.info(s"Waiting for input port for: $fileName")
//      while (!Files.exists(filePath)) {
//        Thread.sleep(UNIT_WAIT_TIME_MS)
//      }
//    }
//    val source = Source.fromFile(fileName)
//    val portNumber = source.mkString.trim.toInt
//    source.close()
    logger.info(s"Input port found at $portNumber")
//    Files.delete(filePath)
    Location.forGrpcInsecure("localhost", portNumber)
  }) ()

  val producer: FlightProducer = new AmberProducer(controlOutputPort, dataOutputPort)
  val server: FlightServer = FlightServer.builder(allocator, location, producer).build()

  override def run(): Unit = {
    server.start()
  }

  @throws[Exception]
  override def close(): Unit = {
    AutoCloseables.close(server, allocator)
  }

}
