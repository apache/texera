package edu.uci.ics.amber.engine.architecture.pythonworker

import akka.actor.ActorRef
import com.softwaremill.macwire.wire
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.{DataOutputPort, NetworkInputPort}
import edu.uci.ics.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.DataElement
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.SendPythonUdfV2
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, ISourceOperatorExecutor}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2

import java.io.IOException
import java.net.ServerSocket
import java.nio.file.Path
import java.util.concurrent.{ExecutorService, Executors}
import scala.sys.process.{BasicIO, Process}

class PythonWorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef) {

  private lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.logger, this.handleDataPayload)
  private lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayload)
  private lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  // Input/Output port used in between Python and Java processes.
  private lazy val inputPortNum: Int = getFreeLocalPort
  private lazy val outputPortNum: Int = getFreeLocalPort
  // Proxy Serve and Client
  private lazy val serverThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private lazy val clientThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private lazy val pythonProxyClient: PythonProxyClient = new PythonProxyClient(outputPortNum)
  private lazy val pythonProxyServer: PythonProxyServer =
    new PythonProxyServer(inputPortNum, controlOutputPort, dataOutputPort)
  // OPTIMIZE: find a way to remove this dependency, AsyncRPC is not used here.
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer = null
  val pythonSrcDirectory: Path = Utils.amberHomePath
    .resolve("src")
    .resolve("main")
    .resolve("python")
  val config: Config = ConfigFactory.load("python_udf")
  val pythonENVPath: String = config.getString("python.path").trim
  // Python process
  private val pythonServerProcess = startPythonProcess()

  override def receive: Receive = {
    disallowActorRefRelatedMessages orElse {
      case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
        dataInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
        controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    pythonProxyClient.enqueueData(DataElement(dataPayload, from))
  }

  final def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    controlPayload match {
      case ControlInvocation(_, _) | ReturnInvocation(_, _) =>
        pythonProxyClient.enqueueCommand(controlPayload, from)
      case _ =>
        logger.logError(
          WorkflowRuntimeError(
            s"unhandled control payload: $controlPayload",
            identifier.toString,
            Map.empty
          )
        )
    }
  }

  override def postStop(): Unit = {

    try {
      // try to send shutdown command so that it can gracefully shutdown
      pythonProxyClient.close()

      clientThreadExecutor.shutdown()

      serverThreadExecutor.shutdown()

      // destroy python process
      pythonServerProcess.destroy()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  override def preStart(): Unit = {
    startProxyServer()
    startProxyClient()
    sendUDF()
  }

  private def sendUDF(): Unit = {
    pythonProxyClient.enqueueCommand(
      ControlInvocationV2(
        -1,
        SendPythonUdfV2(
          udf = operator.asInstanceOf[PythonUDFOpExecV2].getCode,
          isSource = operator.isInstanceOf[ISourceOperatorExecutor]
        )
      ),
      SELF
    )
  }

  private def startProxyServer(): Unit = {
    serverThreadExecutor.submit(pythonProxyServer)
  }

  private def startProxyClient(): Unit = {
    clientThreadExecutor.submit(pythonProxyClient)
  }

  private def startPythonProcess(): Process = {
    val udfMainScriptPath: String = pythonSrcDirectory.resolve("main.py").toString

    Process(
      Seq(
        if (pythonENVPath.isEmpty) "python3"
        else pythonENVPath, // add fall back in case of empty
        "-u",
        udfMainScriptPath,
        Integer.toString(outputPortNum),
        Integer.toString(inputPortNum)
      )
    ).run(BasicIO.standard(false))
  }

  /**
    * Get a random free port.
    *
    * @return The port number.
    * @throws IOException, might happen when getting a free port.
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
