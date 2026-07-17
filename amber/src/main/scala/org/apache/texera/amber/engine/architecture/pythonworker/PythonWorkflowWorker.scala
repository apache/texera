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
import java.util.concurrent.{ExecutorService, Executors}
import scala.sys.process.{BasicIO, Process}

object PythonWorkflowWorker {
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
    super.postStop()
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

  override def initState(): Unit = {
    startProxyServer()
    startPythonProcess()
    startProxyClient()
  }

  private def startProxyServer(): Unit = {
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

  private def startProxyClient(): Unit = {
    clientThreadExecutor.submit(pythonProxyClient)
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

  private def startPythonProcess(): Unit = {
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

    pythonServerProcess = Process(
      Seq(
        pythonBin,
        "-u",
        udfEntryScriptPath,
        PythonWorkflowWorker.encodeStartupConfig(startupConfig)
      )
    ).run(BasicIO.standard(false))
  }

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = ???
}
