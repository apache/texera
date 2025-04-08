package edu.uci.ics.amber.engine.common

import akka.actor.Address
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import java.net.URI

object AmberConfig {

  private val configFile: File = new File("src/main/resources/application.conf")
  private var lastModifiedTime: Long = 0
  private var conf: Config = ConfigFactory.load()

  var masterNodeAddr: Address = Address("akka", "Amber", "localhost", 2552)

  // Perform lazy reload
  private def getConfSource: Config = {
    if (lastModifiedTime == configFile.lastModified()) {
      conf.resolve()
    } else {
      lastModifiedTime = configFile.lastModified()
      conf = ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load())
      conf.resolve()
    }
  }

  // Constants
  val loggingQueueSizeInterval: Int = getConfSource.getInt("constants.logging-queue-size-interval")
  val MAX_RESOLUTION_ROWS: Int = getConfSource.getInt("constants.max-resolution-rows")
  val MAX_RESOLUTION_COLUMNS: Int = getConfSource.getInt("constants.max-resolution-columns")
  val numWorkerPerOperatorByDefault: Int = getConfSource.getInt("constants.num-worker-per-operator")
  val getStatusUpdateIntervalInMs: Long = getConfSource.getLong("constants.status-update-interval")

  // Flow control
  val maxCreditAllowedInBytesPerChannel: Long =
    getConfSource.getLong("flow-control.max-credit-allowed-in-bytes-per-channel") match {
      case -1L       => Long.MaxValue
      case maxCredit => maxCredit
    }

  val creditPollingIntervalInMs: Int =
    getConfSource.getInt("flow-control.credit-poll-interval-in-ms")

  // Network buffering
  val defaultDataTransferBatchSize: Int =
    getConfSource.getInt("network-buffering.default-data-transfer-batch-size")
  val enableAdaptiveNetworkBuffering: Boolean =
    getConfSource.getBoolean("network-buffering.enable-adaptive-buffering")
  val adaptiveBufferingTimeoutMs: Int =
    getConfSource.getInt("network-buffering.adaptive-buffering-timeout-ms")

  // Reconfiguration
  val enableTransactionalReconfiguration: Boolean =
    getConfSource.getBoolean("reconfiguration.enable-transactional-reconfiguration")

  // Fault tolerance
  val faultToleranceLogFlushIntervalInMs: Long =
    getConfSource.getLong("fault-tolerance.log-flush-interval-ms")

  val faultToleranceLogRootFolder: Option[URI] = {
    Option(getConfSource.getString("fault-tolerance.log-storage-uri"))
      .filter(_.nonEmpty)
      .map(new URI(_))
  }

  val isFaultToleranceEnabled: Boolean = faultToleranceLogRootFolder.nonEmpty

  // Scheduling
  val enableCostBasedScheduleGenerator: Boolean =
    getConfSource.getBoolean("schedule-generator.enable-cost-based-schedule-generator")
  val useGlobalSearch: Boolean = getConfSource.getBoolean("schedule-generator.use-global-search")
  val useTopDownSearch: Boolean = getConfSource.getBoolean("schedule-generator.use-top-down-search")
  val searchTimeoutMilliseconds: Int =
    getConfSource.getInt("schedule-generator.search-timeout-milliseconds")

  // Storage cleanup
  val sinkStorageTTLInSecs: Int = getConfSource.getInt("result-cleanup.ttl-in-seconds")
  val sinkStorageCleanUpCheckIntervalInSecs: Int =
    getConfSource.getInt("result-cleanup.collection-check-interval-in-seconds")

  // User system
  val isUserSystemEnabled: Boolean = getConfSource.getBoolean("user-sys.enabled")
  val googleClientId: String = getConfSource.getString("user-sys.google.clientId")
  val gmail: String = getConfSource.getString("user-sys.google.smtp.gmail")
  val smtpPassword: String = getConfSource.getString("user-sys.google.smtp.password")

  // Web server
  val operatorConsoleBufferSize: Int = getConfSource.getInt("web-server.python-console-buffer-size")
  val consoleMessageDisplayLength: Int =
    getConfSource.getInt("web-server.console-message-max-display-length")
  val executionResultPollingInSecs: Int =
    getConfSource.getInt("web-server.workflow-result-pulling-in-seconds")
  val executionStateCleanUpInSecs: Int =
    getConfSource.getInt("web-server.workflow-state-cleanup-in-seconds")
  val workflowVersionCollapseIntervalInMinutes: Int =
    getConfSource.getInt("user-sys.version-time-limit-in-minutes")
  val cleanupAllExecutionResults: Boolean =
    getConfSource.getBoolean("web-server.clean-all-execution-results-on-server-start")

  // AI Assistant
  val aiAssistantConfig: Option[Config] =
    if (getConfSource.hasPath("ai-assistant-server"))
      Some(getConfSource.getConfig("ai-assistant-server"))
    else None
}
