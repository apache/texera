/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.pythonworker

import org.apache.pekko.actor.Props
import com.twitter.util.Promise
import org.apache.texera.common.config.{PythonUtils, StorageConfig, UdfConfig}
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.virtualidentity.ChannelIdentity
import org.apache.texera.amber.engine.architecture.common.WorkflowActor
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkAck
import org.apache.texera.amber.engine.architecture.messaginglayer.{
  NetworkInputGateway,
  NetworkOutputGateway
}
import org.apache.texera.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.{
  DataElement,
  EmbeddedControlMessageElement
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessage
import org.apache.texera.amber.engine.architecture.scheduling.config.WorkerConfig
import org.apache.texera.amber.engine.common.actormessage.{Backpressure, CreditUpdate}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import org.apache.texera.amber.engine.common.ambermessage._
import org.apache.texera.amber.engine.common.{CheckpointState, Utils}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import org.apache.texera.web.resource.pythonvirtualenvironment.PveManager
import java.util.Base64
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.jdk.CollectionConverters._

object PythonWorkflowWorker {

  /** Sent to the worker by its exit watcher when the Python process exits on its own. */
  private[pythonworker] final case class PythonProcessExited(exitCode: Int)

  /** Sent to the worker when its Flight client thread stops on an exception. */
  private[pythonworker] final case class PythonClientFailed(cause: Throwable)

  /**
    * How long a client failure waits for a still-running process to exit before it is reported
    * as a client failure. A killed process's connection can close a moment before the JVM records
    * the exit, and the exit code is the more useful report.
    */
  private val ExitGraceMs = 500L

  /**
    * Why a Python process exited on its own: before it reported its port (`connected` false) or
    * after. 137 is 128 + SIGKILL, what the OOM killer sends.
    */
  private[pythonworker] def describeUnexpectedExit(
      workerName: String,
      exitCode: Int,
      connected: Boolean
  ): String = {
    val hint =
      if (exitCode == 137) " (killed by SIGKILL, often the out-of-memory killer)"
      else ""
    if (connected)
      s"Python worker process for $workerName exited unexpectedly with code $exitCode$hint"
    else
      s"Python worker process for $workerName exited with code $exitCode$hint before connecting to the engine"
  }

  def props(workerConfig: WorkerConfig): Props = Props(new PythonWorkflowWorker(workerConfig))

  /**
    * Serialize the Python worker startup configuration to a JSON object keyed by
    * name, then Base64-encode it for safe passing as a command-line argument. Built
    * from a sequence of (key, value) pairs so a duplicate key fails loudly here
    * instead of being silently dropped by Map construction.
    *
    * The Base64 step matters on Windows: a raw JSON string passed as argv loses its
    * quotes there (the JVM assembles argv into a single command line and the inner
    * double quotes are stripped before Python receives it), so `json.loads` fails.
    * Base64 uses only `[A-Za-z0-9+/=]`, which survives argv quoting on every
    * platform. The Python side Base64-decodes before parsing the JSON.
    */
  def encodeStartupConfig(entries: Seq[(String, String)]): String = {
    val duplicateKeys = entries.groupBy(_._1).collect { case (key, group) if group.size > 1 => key }
    require(
      duplicateKeys.isEmpty,
      s"duplicate Python worker startup config keys: ${duplicateKeys.mkString(", ")}"
    )
    val json = objectMapper.writeValueAsString(entries.toMap)
    Base64.getEncoder.encodeToString(json.getBytes(StandardCharsets.UTF_8))
  }

  /**
    * Assemble the Python worker startup configuration as named (key, value) pairs.
    * Worker-specific values are passed in; storage-related values are read from the
    * shared StorageConfig (Postgres/REST catalog fields are blank unless that catalog
    * type is active). Returned as a sequence (not a Map) so encodeStartupConfig can
    * detect a duplicate key.
    */
  def buildStartupConfig(
      workerId: String,
      outputPort: String,
      rPath: String,
      largeBinaryBaseUri: String
  ): Seq[(String, String)] = {
    val isPostgres = StorageConfig.icebergCatalogType == "postgres"
    val isRest = StorageConfig.icebergCatalogType == "rest"
    Seq(
      "workerId" -> workerId,
      "outputPort" -> outputPort,
      "loggerLevel" -> UdfConfig.pythonLogStreamHandlerLevel,
      "rPath" -> rPath,
      "icebergCatalogType" -> StorageConfig.icebergCatalogType,
      "icebergPostgresCatalogUriWithoutScheme" ->
        (if (isPostgres) StorageConfig.icebergPostgresCatalogUriWithoutScheme else ""),
      "icebergPostgresCatalogUsername" ->
        (if (isPostgres) StorageConfig.icebergPostgresCatalogUsername else ""),
      "icebergPostgresCatalogPassword" ->
        (if (isPostgres) StorageConfig.icebergPostgresCatalogPassword else ""),
      "icebergRestCatalogUri" -> (if (isRest) StorageConfig.icebergRESTCatalogUri else ""),
      "icebergRestCatalogWarehouseName" ->
        (if (isRest) StorageConfig.icebergRESTCatalogWarehouseName else ""),
      "icebergTableNamespace" -> StorageConfig.icebergTableResultNamespace,
      "icebergTableStateNamespace" -> StorageConfig.icebergTableStateNamespace,
      "icebergFileStorageDirectoryPath" -> StorageConfig.fileStorageDirectoryPath.toString,
      "icebergTableCommitBatchSize" -> StorageConfig.icebergTableCommitBatchSize.toString,
      "s3Endpoint" -> StorageConfig.s3Endpoint,
      "s3Region" -> StorageConfig.s3Region,
      "s3AuthUsername" -> StorageConfig.s3Username,
      "s3AuthPassword" -> StorageConfig.s3Password,
      "s3LargeBinariesBaseUri" -> largeBinaryBaseUri
    )
  }
}

class PythonWorkflowWorker(
    workerConfig: WorkerConfig
) extends WorkflowActor(replayLogConfOpt = None, actorId = workerConfig.workerId) {

  // For receiving the Python server port number that will be available later
  private lazy val portNumberPromise = Promise[Int]()
  // Proxy Server and Client
  private lazy val serverThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private lazy val clientThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private var pythonProxyServer: PythonProxyServer = _
  private lazy val pythonProxyClient: PythonProxyClient =
    new PythonProxyClient(portNumberPromise, workerConfig.workerId)

  val pythonSrcDirectory: Path = Utils.amberHomePath
    .resolve("src")
    .resolve("main")
    .resolve("python")
  val RENVPath: String = UdfConfig.rPath.trim

  // Python process
  private var pythonServerProcess: Process = _
  // Set before this actor tears the Python side down, so the exits that follow are not reported.
  @volatile private var stopping = false

  private val networkInputGateway = new NetworkInputGateway(workerConfig.workerId)
  private val networkOutputGateway = new NetworkOutputGateway(
    workerConfig.workerId,
    // handler for output messages
    msg => {
      logManager.sendCommitted(Right(msg))
    }
  )

  override def handleInputMessage(messageId: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = networkInputGateway.getChannel(workflowMsg.channelId)
    channel.acceptMessage(workflowMsg)
    while (channel.isEnabled && channel.hasMessage) {
      val msg = channel.take
      msg.payload match {
        case payload: DirectControlMessagePayload =>
          pythonProxyClient.enqueueCommand(payload, workflowMsg.channelId)
        case payload: DataPayload =>
          pythonProxyClient.enqueueData(DataElement(payload, workflowMsg.channelId))
        case ecm: EmbeddedControlMessage =>
          pythonProxyClient.enqueueData(EmbeddedControlMessageElement(ecm, workflowMsg.channelId))
        case p => logger.error(s"unhandled control payload: $p")
      }
    }
    sender() ! NetworkAck(
      messageId,
      getInMemSize(workflowMsg),
      getQueuedCredit(workflowMsg.channelId)
    )
  }

  override def receive: Receive = handlePythonFailure orElse super.receive

  // Throwing hands the failure to the coordinator's supervisor, which fails the execution.
  private def handlePythonFailure: Receive = {
    case PythonWorkflowWorker.PythonProcessExited(code) =>
      throw new WorkflowRuntimeException(
        PythonWorkflowWorker.describeUnexpectedExit(
          workerConfig.workerId.name,
          code,
          portNumberPromise.isDefined
        ),
        Some(workerConfig.workerId)
      )
    case PythonWorkflowWorker.PythonClientFailed(cause) =>
      val process = pythonServerProcess
      // When the process is gone, its exit is the reason the client stopped; report that.
      val message =
        if (
          process != null &&
          process.waitFor(PythonWorkflowWorker.ExitGraceMs, TimeUnit.MILLISECONDS)
        ) {
          PythonWorkflowWorker.describeUnexpectedExit(
            workerConfig.workerId.name,
            process.exitValue(),
            portNumberPromise.isDefined
          )
        } else {
          s"Flight client for Python worker ${workerConfig.workerId.name} stopped: $cause"
        }
      throw new WorkflowRuntimeException(message, cause, Some(workerConfig.workerId))
  }

  override def receiveCreditMessages: Receive = {
    case WorkflowActor.CreditRequest(channel) =>
      pythonProxyClient.enqueueActorCommand(CreditUpdate())
      sender() ! WorkflowActor.CreditResponse(channel, getQueuedCredit(channel))
    case WorkflowActor.CreditResponse(channel, credit) =>
      transferService.updateChannelCreditFromReceiver(channel, credit)
  }

  /** flow-control */
  override def getQueuedCredit(channelId: ChannelIdentity): Long = {
    pythonProxyClient.getQueuedCredit(channelId) + pythonProxyClient.getQueuedCredit
  }

  override def handleBackpressure(enableBackpressure: Boolean): Unit = {
    pythonProxyClient.enqueueActorCommand(Backpressure(enableBackpressure))
  }

  override def postStop(): Unit = {
    stopping = true
    super.postStop()
    try {
      // try to send shutdown command so that it can gracefully shutdown. A client still waiting
      // for Python's port stops waiting.
      pythonProxyClient.close()

      clientThreadExecutor.shutdown()

      serverThreadExecutor.shutdown()

      // destroy python process, if it was started
      if (pythonServerProcess != null) pythonServerProcess.destroy()
    } catch {
      case e: Exception =>
        logger.error(s"$e - happened during shutdown")
    }
  }

  // The client waits for Python's port on its own thread, so start-up does not block on Python
  // and the worker registers with its parent before any failure of Python can be reported.
  override def initState(): Unit = {
    startProxyServer()
    startPythonProcess()
    startProxyClient()
  }

  private[pythonworker] def startProxyServer(): Unit = {
    // Try to start the server until it succeeds
    var serverStart = false
    while (!serverStart) {
      pythonProxyServer =
        new PythonProxyServer(networkOutputGateway, workerConfig.workerId, portNumberPromise)
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

  private[pythonworker] def startProxyClient(): Unit = {
    val client = pythonProxyClient
    // Nothing reads the Future that submit returns, so failures are reported from here.
    clientThreadExecutor.submit(new Runnable {
      override def run(): Unit =
        try client.run()
        catch {
          case t: Throwable =>
            if (!stopping) self ! PythonWorkflowWorker.PythonClientFailed(t)
        }
    })
  }

  // Returns the Python executable path for the selected PVE,
  // or falls back to the default Python binary.
  private def choosePythonBin(): String = {
    val fallback = PythonUtils.getPythonExecutable
    val pveName = workerConfig.pveName.trim

    workerConfig.cuid
      .filter(_ => pveName.nonEmpty)
      .flatMap(cuid => PveManager.getPythonBin(cuid, pveName))
      .map(_.toString)
      .getOrElse(fallback)
  }

  private[pythonworker] def startPythonProcess(): Unit = {
    val udfEntryScriptPath: String =
      pythonSrcDirectory.resolve("texera_run_python_worker.py").toString

    val pythonBin: String = choosePythonBin()

    // Pass startup configuration to the Python worker by name, as a single JSON
    // object, rather than by argv position. This way the two sides agree by key,
    // so adding/removing/reordering a field can no longer silently misassign
    // values; a missing or renamed key fails loudly on the Python side instead.
    val startupConfig = PythonWorkflowWorker.buildStartupConfig(
      workerConfig.workerId.name,
      Integer.toString(pythonProxyServer.getPortNumber.get()),
      RENVPath,
      workerConfig.largeBinaryBaseUri
    )

    // Python writes straight to this JVM's stdout and stderr, with no copying threads in between:
    // its exit is then known when the process ends, even if a child it started (a UDF's
    // subprocess, for example) still holds those streams open.
    pythonServerProcess = new ProcessBuilder(
      Seq(
        pythonBin,
        "-u",
        udfEntryScriptPath,
        PythonWorkflowWorker.encodeStartupConfig(startupConfig)
      ).asJava
    ).redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
      .start()
    pythonServerProcess.getOutputStream.close() // the worker reads no input
    watchPythonProcess()
  }

  /** Reports an exit of the Python process that this actor did not cause. */
  private def watchPythonProcess(): Unit = {
    val process = pythonServerProcess
    val watcher = new Thread(
      () =>
        try {
          val code = process.waitFor()
          if (!stopping) self ! PythonWorkflowWorker.PythonProcessExited(code)
        } catch {
          case _: InterruptedException => // interrupted while shutting down; nothing to report
        },
      s"python-exit-watcher-${workerConfig.workerId.name}"
    )
    watcher.setDaemon(true)
    watcher.start()
  }

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = ???
}
