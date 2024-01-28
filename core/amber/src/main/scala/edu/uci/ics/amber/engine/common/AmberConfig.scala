package edu.uci.ics.amber.engine.common

import akka.actor.Address
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.texera.Utils

import java.io.File
import java.net.URI

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
  val loggingQueueSizeInterval: Int = getConfSource.getInt("constants.logging-queue-size-interval")
  val MAX_RESOLUTION_ROWS: Int = getConfSource.getInt("constants.max-resolution-rows")
  val MAX_RESOLUTION_COLUMNS: Int = getConfSource.getInt("constants.max-resolution-columns")
  val numWorkerPerOperatorByDefault: Int = getConfSource.getInt("constants.num-worker-per-operator")
  val getStatusUpdateIntervalInMs: Long = getConfSource.getLong("constants.status-update-interval")

  // Monitoring and reshape related configuration
  val monitoringEnabled: Boolean =
    getConfSource.getBoolean("monitoring.monitoring-enabled")
  val monitoringIntervalInMs: Int =
    getConfSource.getInt("monitoring.monitoring-interval-ms")
  val reshapeSkewHandlingEnabled: Boolean =
    getConfSource.getBoolean("reshape.skew-handling-enabled")
  val reshapeSkewDetectionInitialDelayInMs: Int =
    getConfSource.getInt("reshape.skew-detection-initial-delay-ms")
  val reshapeSkewDetectionIntervalInMs: Int =
    getConfSource.getInt("reshape.skew-detection-interval-ms")
  val reshapeEtaThreshold: Int =
    getConfSource.getInt("reshape.eta-threshold")
  val reshapeTauThreshold: Int =
    getConfSource.getInt("reshape.tau-threshold")
  val reshapeHelperOverloadThreshold: Int =
    getConfSource.getInt("reshape.helper-overload-threshold")
  val reshapeMaxWorkloadSamplesInController: Int =
    getConfSource.getInt("reshape.max-workload-samples-controller")
  val reshapeMaxWorkloadSamplesInWorker: Int =
    getConfSource.getInt("reshape.max-workload-samples-worker")
  val reshapeWorkloadSampleSize: Int =
    getConfSource.getInt("reshape.workload-sample-size")
  val reshapeFirstPhaseSharingNumerator: Int =
    getConfSource.getInt("reshape.first-phase-sharing-numerator")
  val reshapeFirstPhaseSharingDenominator: Int =
    getConfSource.getInt("reshape.first-phase-sharing-denominator")

  // Flow control related configuration
  def maxCreditAllowedInBytesPerChannel: Long = {
    val maxCredit = getConfSource.getLong("flow-control.max-credit-allowed-in-bytes-per-channel")
    if (maxCredit == -1L) Long.MaxValue else maxCredit
  }
  val creditPollingIntervalInMs: Int =
    getConfSource.getInt("flow-control.credit-poll-interval-in-ms")

  // Scheduling related configuration
  val schedulingPolicyName: String = getConfSource.getString("scheduling.policy-name")
  val timeSlotExpirationDurationInMs: Int =
    getConfSource.getInt("scheduling.time-slot-expiration-duration-ms")

  // Network buffering configuration
  def defaultBatchSize: Int = getConfSource.getInt("network-buffering.default-batch-size")
  val enableAdaptiveNetworkBuffering: Boolean =
    getConfSource.getBoolean("network-buffering.enable-adaptive-buffering")
  val adaptiveBufferingTimeoutMs: Int =
    getConfSource.getInt("network-buffering.adaptive-buffering-timeout-ms")

  // Fries configuration
  val enableTransactionalReconfiguration: Boolean =
    getConfSource.getBoolean("reconfiguration.enable-transactional-reconfiguration")

  // Fault tolerance configuration
  val faultToleranceLogFlushIntervalInMs: Long =
    getConfSource.getLong("fault-tolerance.log-flush-interval-ms")
  val faultToleranceLogRootFolder: Option[URI] = {
    var locationStr = getConfSource.getString("fault-tolerance.log-storage-uri").trim
    if (locationStr.isEmpty) {
      None
    } else {
      if (locationStr.contains("$AMBER_FOLDER")) {
        assert(locationStr.startsWith("file"))
        locationStr =
          locationStr.replace("$AMBER_FOLDER", Utils.amberHomePath.toAbsolutePath.toString)
      }
      Some(new URI(locationStr))
    }
  }
  val isFaultToleranceEnabled: Boolean = faultToleranceLogRootFolder.nonEmpty

  // Storage configuration
  val sinkStorageMode: String = getConfSource.getString("storage.mode")
  val sinkStorageMongoDBConfig: Config = getConfSource.getConfig("storage.mongodb")
  val sinkStorageTTLInSecs: Int = getConfSource.getInt("result-cleanup.ttl-in-seconds")
  val sinkStorageCleanUpCheckIntervalInSecs: Int =
    getConfSource.getInt("result-cleanup.collection-check-interval-in-seconds")

  // User system and authentication configuration
  val isUserSystemEnabled: Boolean = getConfSource.getBoolean("user-sys.enabled")
  val jWTConfig: Config = getConfSource.getConfig("user-sys.jwt")
  val googleClientId: String = getConfSource.getString("user-sys.google.clientId")
  val googleClientSecret: String = getConfSource.getString("user-sys.google.clientSecret")

  // Web server configuration
  val operatorConsoleBufferSize: Int = getConfSource.getInt("web-server.python-console-buffer-size")
  val executionResultPollingInSecs: Int =
    getConfSource.getInt("web-server.workflow-result-pulling-in-seconds")
  val executionStateCleanUpInSecs: Int =
    getConfSource.getInt("web-server.workflow-state-cleanup-in-seconds")
  val workflowVersionCollapseIntervalInMinutes: Int =
    getConfSource.getInt("user-sys.version-time-limit-in-minutes")

  // JDBC configuration
  val jdbcConfig: Config = getConfSource.getConfig("jdbc")

}
