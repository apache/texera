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
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.{ExecutorService, Executors}

class PythonWorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef) {

  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.logger, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayload)

  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]

  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer = null

  val config: Config = ConfigFactory.load("python_udf")
  val pythonPath: String = config.getString("python.path").trim

  val inputPortNum: Int = getFreeLocalPort
  val outputPortNum: Int = getFreeLocalPort

  private val serverThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private val clientThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor

  private val pythonProxyClient: PythonProxyClient = new PythonProxyClient(outputPortNum)
  private val pythonProxyServer: PythonProxyServer =
    new PythonProxyServer(inputPortNum, controlOutputPort, dataOutputPort)

  private var pythonServerProcess: Process = _

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
      pythonServerProcess.destroyForcibly()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  override def preStart(): Unit = {
    val udfMainScriptPath =
      "/Users/yicong-huang/IdeaProjects/texera-other/core/amber/src/main/python/main.py"
    // TODO: find a better way to do default conf values.

    startProxyServer()

    startPythonProcess(udfMainScriptPath)

    startProxyClient()

    sendUDF()
  }

  def sendUDF(): Unit = {
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

  private def startPythonProcess(
      udfMainScriptPath: String
  ): Unit = {
    pythonServerProcess = new ProcessBuilder(
      if (pythonPath.isEmpty) "python3"
      else pythonPath, // add fall back in case of empty
      "-u",
      udfMainScriptPath,
      Integer.toString(outputPortNum),
      Integer.toString(inputPortNum)
    ).inheritIO.start
  }

  def startProxyServer(): Unit = {
    serverThreadExecutor.submit(pythonProxyServer)
  }

  def startProxyClient(): Unit = {
    clientThreadExecutor.submit(pythonProxyClient)
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
