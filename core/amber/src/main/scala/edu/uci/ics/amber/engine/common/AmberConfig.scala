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
      conf
    } else {
      lastModifiedTime = configFile.lastModified()
      conf = ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load())
      conf
    }
  }

  // Constants
  def loggingQueueSizeInterval: Int = getConfSource.getInt("constants.logging-queue-size-interval")
  def MAX_RESOLUTION_ROWS: Int = getConfSource.getInt("constants.max-resolution-rows")
  def MAX_RESOLUTION_COLUMNS: Int = getConfSource.getInt("constants.max-resolution-columns")
  def numWorkerPerOperatorByDefault: Int = getConfSource.getInt("constants.num-worker-per-operator")
  def getStatusUpdateIntervalInMs: Long = getConfSource.getLong("constants.status-update-interval")

  // Flow control
  def maxCreditAllowedInBytesPerChannel: Long =
    getConfSource.getLong("flow-control.max-credit-allowed-in-bytes-per-channel") match {
      case -1L       => Long.MaxValue
      case maxCredit => maxCredit
    }

  def creditPollingIntervalInMs: Int =
    getConfSource.getInt("flow-control.credit-poll-interval-in-ms")

  // Network buffering
  def defaultDataTransferBatchSize: Int =
    getConfSource.getInt("network-buffering.default-data-transfer-batch-size")
  def enableAdaptiveNetworkBuffering: Boolean =
    getConfSource.getBoolean("network-buffering.enable-adaptive-buffering")
  def adaptiveBufferingTimeoutMs: Int =
    getConfSource.getInt("network-buffering.adaptive-buffering-timeout-ms")

  // Reconfiguration
  def enableTransactionalReconfiguration: Boolean =
    getConfSource.getBoolean("reconfiguration.enable-transactional-reconfiguration")

  // Fault tolerance
  def faultToleranceLogFlushIntervalInMs: Long =
    getConfSource.getLong("fault-tolerance.log-flush-interval-ms")

  def faultToleranceLogRootFolder: Option[URI] = {
    Option(getConfSource.getString("fault-tolerance.log-storage-uri"))
      .filter(_.nonEmpty)
      .map(new URI(_))
  }

  def isFaultToleranceEnabled: Boolean = faultToleranceLogRootFolder.nonEmpty

  // Scheduling
  def enableCostBasedScheduleGenerator: Boolean =
    getConfSource.getBoolean("schedule-generator.enable-cost-based-schedule-generator")
  def useGlobalSearch: Boolean = getConfSource.getBoolean("schedule-generator.use-global-search")
  def useTopDownSearch: Boolean = getConfSource.getBoolean("schedule-generator.use-top-down-search")
  def searchTimeoutMilliseconds: Int =
    getConfSource.getInt("schedule-generator.search-timeout-milliseconds")

  // Storage cleanup
  def sinkStorageTTLInSecs: Int = getConfSource.getInt("result-cleanup.ttl-in-seconds")
  def sinkStorageCleanUpCheckIntervalInSecs: Int =
    getConfSource.getInt("result-cleanup.collection-check-interval-in-seconds")

  // User system
  def isUserSystemEnabled: Boolean = getConfSource.getBoolean("user-sys.enabled")
  def jWTConfig: Config = getConfSource.getConfig("user-sys.jwt")
  def googleClientId: String = getConfSource.getString("user-sys.google.clientId")
  def gmail: String = getConfSource.getString("user-sys.google.smtp.gmail")
  def smtpPassword: String = getConfSource.getString("user-sys.google.smtp.password")

  // Web server
  def operatorConsoleBufferSize: Int = getConfSource.getInt("web-server.python-console-buffer-size")
  def executionResultPollingInSecs: Int =
    getConfSource.getInt("web-server.workflow-result-pulling-in-seconds")
  def executionStateCleanUpInSecs: Int =
    getConfSource.getInt("web-server.workflow-state-cleanup-in-seconds")
  def workflowVersionCollapseIntervalInMinutes: Int =
    getConfSource.getInt("user-sys.version-time-limit-in-minutes")
  def cleanupAllExecutionResults: Boolean =
    getConfSource.getBoolean("web-server.clean-all-execution-results-on-server-start")

  // AI Assistant
  def aiAssistantConfig: Option[Config] =
    if (getConfSource.hasPath("ai-assistant-server"))
      Some(getConfSource.getConfig("ai-assistant-server"))
    else None
}
