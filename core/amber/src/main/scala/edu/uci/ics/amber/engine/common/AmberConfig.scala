package edu.uci.ics.amber.engine.common

import akka.actor.Address
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.texera.Utils
import java.io.File

object AmberConfig {

  private val configFile: File = Utils.amberHomePath
    .resolve("src/main/resources/application.conf")
    .toFile
  private var lastModifiedTime: Long = 0
  private var conf: Config = _

  var masterNodeAddr: Address = Address("akka", "Amber", "localhost", 2552)

  // perform lazy reload
  private def getConfSource: Config = {
    if (lastModifiedTime == configFile.lastModified()) {
      conf
    } else {
      lastModifiedTime = configFile.lastModified()
      conf = ConfigFactory.parseFile(configFile)
      conf
    }
  }

  // Constants configuration
  def loggingQueueSizeInterval: Int = getConfSource.getInt("constants.logging-queue-size-interval")
  def MAX_RESOLUTION_ROWS: Int = getConfSource.getInt("constants.max-resolution-rows")
  def MAX_RESOLUTION_COLUMNS: Int = getConfSource.getInt("constants.max-resolution-columns")
  def numWorkerPerOperatorByDefault: Int = getConfSource.getInt("constants.num-worker-per-operator")
  def getStatusUpdateIntervalInMs: Long = getConfSource.getLong("constants.status-update-interval")

  // Monitoring and reshape related configuration
  def monitoringEnabled: Boolean =
    getConfSource.getBoolean("monitoring.monitoring-enabled")
  def monitoringIntervalInMs: Int =
    getConfSource.getInt("monitoring.monitoring-interval-ms")
  def reshapeSkewHandlingEnabled: Boolean =
    getConfSource.getBoolean("reshape.skew-handling-enabled")
  def reshapeSkewDetectionInitialDelayInMs: Int =
    getConfSource.getInt("reshape.skew-detection-initial-delay-ms")
  def reshapeSkewDetectionIntervalInMs: Int =
    getConfSource.getInt("reshape.skew-detection-interval-ms")
  def reshapeEtaThreshold: Int =
    getConfSource.getInt("reshape.eta-threshold")
  def reshapeTauThreshold: Int =
    getConfSource.getInt("reshape.tau-threshold")
  def reshapeHelperOverloadThreshold: Int =
    getConfSource.getInt("reshape.helper-overload-threshold")
  def reshapeMaxWorkloadSamplesInController: Int =
    getConfSource.getInt("reshape.max-workload-samples-controller")
  def reshapeMaxWorkloadSamplesInWorker: Int =
    getConfSource.getInt("reshape.max-workload-samples-worker")
  def reshapeWorkloadSampleSize: Int =
    getConfSource.getInt("reshape.workload-sample-size")
  def reshapeFirstPhaseSharingNumerator: Int =
    getConfSource.getInt("reshape.first-phase-sharing-numerator")
  def reshapeFirstPhaseSharingDenominator: Int =
    getConfSource.getInt("reshape.first-phase-sharing-denominator")

  // Flow control related configuration
  def maxCreditAllowedInBytesPerChannel: Long = {
    val maxCredit = getConfSource.getLong("flow-control.max-credit-allowed-in-bytes-per-channel")
    if (maxCredit == -1L) Long.MaxValue else maxCredit
  }
  def creditPollingIntervalInMs: Int =
    getConfSource.getInt("flow-control.credit-poll-interval-in-ms")

  // Scheduling related configuration
  def schedulingPolicyName: String = getConfSource.getString("scheduling.policy-name")
  def timeSlotExpirationDurationInMs: Int =
    getConfSource.getInt("scheduling.time-slot-expiration-duration-ms")

  // Network buffering configuration
  def defaultBatchSize: Int = getConfSource.getInt("network-buffering.default-batch-size")
  def enableAdaptiveNetworkBuffering: Boolean =
    getConfSource.getBoolean("network-buffering.enable-adaptive-buffering")
  def adaptiveBufferingTimeoutMs: Int =
    getConfSource.getInt("network-buffering.adaptive-buffering-timeout-ms")

  // Fries configuration
  def enableTransactionalReconfiguration: Boolean =
    getConfSource.getBoolean("reconfiguration.enable-transactional-reconfiguration")

  // Fault tolerance configuration
  def isFaultToleranceEnabled: Boolean =
    getConfSource.getBoolean("fault-tolerance.enable-determinant-logging")
  def delayBeforeRecovery: Long = getConfSource.getLong("fault-tolerance.delay-before-recovery")
  def faultToleranceLogFlushIntervalInMs: Long =
    getConfSource.getLong("fault-tolerance.log-flush-interval-ms")
  def faultToleranceLogStorage: String = getConfSource.getString("fault-tolerance.log-storage-type")
  def faultToleranceHDFSAddress: String =
    getConfSource.getString("fault-tolerance.hdfs-storage.address")

  // Storage configuration
  def sinkStorageMode: String = getConfSource.getString("storage.mode")
  def sinkStorageMongoDBConfig: Config = getConfSource.getConfig("storage.mongodb")
  def sinkStorageTTLInSecs: Int = getConfSource.getInt("result-cleanup.ttl-in-seconds")
  def sinkStorageCleanUpCheckIntervalInSecs: Int =
    getConfSource.getInt("result-cleanup.collection-check-interval-in-seconds")

  // User system and authentication configuration
  def isUserSystemEnabled: Boolean = getConfSource.getBoolean("user-sys.enabled")
  def jWTConfig: Config = getConfSource.getConfig("user-sys.jwt")
  def googleClientId: String = getConfSource.getString("user-sys.google.clientId")
  def googleClientSecret: String = getConfSource.getString("user-sys.google.clientSecret")

  // Web server configuration
  def operatorConsoleBufferSize: Int = getConfSource.getInt("web-server.python-console-buffer-size")
  def executionResultPollingInSecs: Int =
    getConfSource.getInt("web-server.workflow-result-pulling-in-seconds")
  def executionStateCleanUpInSecs: Int =
    getConfSource.getInt("web-server.workflow-state-cleanup-in-seconds")
  def workflowVersionCollapseIntervalInMinutes: Int =
    getConfSource.getInt("user-sys.version-time-limit-in-minutes")

  // JDBC configuration
  def jdbcConfig: Config = getConfSource.getConfig("jdbc")

}
