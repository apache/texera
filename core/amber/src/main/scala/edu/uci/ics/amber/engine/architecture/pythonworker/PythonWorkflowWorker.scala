package edu.uci.ics.amber.engine.architecture.pythonworker

import akka.actor.Props
import com.twitter.util.Promise
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.DataElement
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.BackpressureHandler.Backpressure
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.Utils

import java.nio.file.Path
import java.util.concurrent.{ExecutorService, Executors}
import scala.sys.process.{BasicIO, Process}

object PythonWorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      workerIndex: Int,
      workerLayer: OpExecConfig,
      parentNetworkCommunicationActorRef: NetworkSenderActorRef
  ): Props =
    Props(
      new PythonWorkflowWorker(
        id,
        workerIndex,
        workerLayer,
        parentNetworkCommunicationActorRef
      )
    )
}

class PythonWorkflowWorker(
    actorId: ActorVirtualIdentity,
    workerIndex: Int,
    workerLayer: OpExecConfig,
    parentNetworkCommunicationActorRef: NetworkSenderActorRef
) extends WorkflowWorker(
      actorId,
      workerIndex,
      workerLayer,
      parentNetworkCommunicationActorRef,
      false
    ) {

  // For receiving the Python server port number that will be available later
  private lazy val portNumberPromise = Promise[Int]()
  // Proxy Server and Client
  private lazy val serverThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private lazy val clientThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private var pythonProxyServer: PythonProxyServer = _
  private lazy val pythonProxyClient: PythonProxyClient =
    new PythonProxyClient(portNumberPromise, actorId)

  // TODO: find a better way to send Error log to frontend.
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer = null

  val pythonSrcDirectory: Path = Utils.amberHomePath
    .resolve("src")
    .resolve("main")
    .resolve("python")
  val config: Config = ConfigFactory.load("python_udf")
  val pythonENVPath: String = config.getString("python.path").trim
  // Python process
  private var pythonServerProcess: Process = _

  // TODO: Implement credit calculation logic in python worker
  override def getSenderCredits(sender: ActorVirtualIdentity) = {
    Constants.unprocessedBatchesCreditLimitPerSender
  }

  override def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    pythonProxyClient.enqueueData(DataElement(dataPayload, from))
  }

  override def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    controlPayload match {
      case ControlInvocation(_, c) =>
        // TODO: Implement backpressure message handling for python worker
        if (!c.isInstanceOf[Backpressure]) {
          pythonProxyClient.enqueueCommand(controlPayload, from)
        }
      case ReturnInvocation(_, _) =>
        pythonProxyClient.enqueueCommand(controlPayload, from)
      case _ =>
        logger.error(s"unhandled control payload: $controlPayload")
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
        logger.error(s"$e - happened during shutdown")
    }
  }

  override def preStart(): Unit = {
    startProxyServer()
    startPythonProcess()
    startProxyClient()
  }

  private def startProxyServer(): Unit = {
    // Try to start the server until it succeeds
    var serverStart = false
    while (!serverStart) {
      pythonProxyServer =
        new PythonProxyServer(controlOutputPort, dataOutputPort, actorId, portNumberPromise)
      val future = serverThreadExecutor.submit(pythonProxyServer)
      try {
        future.get()
        serverStart = true
      } catch {
        case e: Exception =>
          future.cancel(true)
          logger.info("Failed to start the server: " + e.getMessage + ", will try again")
      }
    }
  }

  private def startProxyClient(): Unit = {
    clientThreadExecutor.submit(pythonProxyClient)
  }

  private def startPythonProcess(): Unit = {
    val udfEntryScriptPath: String =
      pythonSrcDirectory.resolve("texera_run_python_worker.py").toString
    pythonServerProcess = Process(
      Seq(
        if (pythonENVPath.isEmpty) "python3"
        else pythonENVPath, // add fall back in case of empty
        "-u",
        udfEntryScriptPath,
        actorId.name,
        Integer.toString(pythonProxyServer.getPortNumber.get()),
        config.getString("python.log.streamHandler.level")
      )
    ).run(BasicIO.standard(false))
  }
}
