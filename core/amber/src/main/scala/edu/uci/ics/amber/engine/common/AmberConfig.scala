package edu.uci.ics.amber.engine.common

import akka.actor.Address
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.texera.Utils

import java.io.File
import java.net.URI

import java.security.MessageDigest

object MerkleTreeFromByteArray {

  // Function to hash a byte array using SHA-256
  def sha256Hash(data: Array[Byte]): Array[Byte] = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(data)
  }

  // Function to combine two hashes (byte arrays)
  def combineHash(left: Array[Byte], right: Array[Byte]): Array[Byte] = {
    sha256Hash(left ++ right) // Concatenate two byte arrays and hash the result
  }

  // Function to split the byte array into chunks
  def splitByteArray(data: Array[Byte], chunkSize: Int): List[Array[Byte]] = {
    data.grouped(chunkSize).toList
  }

  // Entry point method to build the Merkle tree from a byte array
  def buildTree(data: Array[Byte], chunkSize: Int): Array[Byte] = {
    // Split the data into chunks
    val dataBlocks = splitByteArray(data, chunkSize)
    // Hash each chunk
    val hashedBlocks = dataBlocks.map(sha256Hash)
    // Build the Merkle tree and return the root hash
    buildMerkleTree(hashedBlocks)
  }

  // Function to build the Merkle tree recursively (with byte arrays)
  private def buildMerkleTree(dataBlocks: List[Array[Byte]]): Array[Byte] = {
    if (dataBlocks.length == 1) return dataBlocks.head

    // Process pairs of data blocks to create parent hashes
    val parentLevel = dataBlocks.grouped(2).map {
      case List(left, right) => combineHash(left, right)
      case List(left) => combineHash(left, left) // Handle odd number of nodes by duplicating the last node
    }.toList

    // Recursively build the tree
    buildMerkleTree(parentLevel)
  }

  def getDiffInBytes(oldData: Array[Byte], newData: Array[Byte], chunkSize: Int): Int = {
    // Split both old and new data into chunks of size `chunkSize`
    val oldChunks = splitByteArray(oldData, chunkSize)
    val newChunks = splitByteArray(newData, chunkSize)

    // Hash each chunk
    val oldHashedChunks = oldChunks.map(sha256Hash)
    val newHashedChunks = newChunks.map(sha256Hash)

    // Compare the old and new hashed blocks to find the differences
    val differingBlocks = oldHashedChunks.zip(newHashedChunks).count {
      case (oldHash, newHash) => !oldHash.sameElements(newHash)
    }

    // Return the number of differing bytes (differing blocks * chunkSize)
    differingBlocks * chunkSize
  }
}


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
  def numWorkerPerOperatorByDefault: Int = getConfSource.getInt("constants.num-worker-per-operator")
  def getStatusUpdateIntervalInMs: Long = getConfSource.getLong("constants.interaction-interval")

  // Flow control related configuration
  def maxCreditAllowedInBytesPerChannel: Long = {
    val maxCredit = getConfSource.getLong("flow-control.max-credit-allowed-in-bytes-per-channel")
    if (maxCredit == -1L) Long.MaxValue else maxCredit
  }
  val creditPollingIntervalInMs: Int =
    getConfSource.getInt("flow-control.credit-poll-interval-in-ms")

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

  def faultToleranceLogRootFolder: Option[URI] = {
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
  def isFaultToleranceEnabled: Boolean = faultToleranceLogRootFolder.nonEmpty

  // Region plan generator
  val enableCostBasedRegionPlanGenerator: Boolean =
    getConfSource.getBoolean("region-plan-generator.enable-cost-based-region-plan-generator")
  val useGlobalSearch: Boolean = getConfSource.getBoolean("region-plan-generator.use-global-search")

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
  val gmail: String = getConfSource.getString("user-sys.google.smtp.gmail")
  val smtpPassword: String = getConfSource.getString("user-sys.google.smtp.password")

  // Web server configuration
  val operatorConsoleBufferSize: Int = getConfSource.getInt("web-server.python-console-buffer-size")
  val executionResultPollingInSecs: Int =
    getConfSource.getInt("web-server.workflow-result-pulling-in-seconds")
  val executionStateCleanUpInSecs: Int =
    getConfSource.getInt("web-server.workflow-state-cleanup-in-seconds")
  val workflowVersionCollapseIntervalInMinutes: Int =
    getConfSource.getInt("user-sys.version-time-limit-in-minutes")
  val cleanupAllExecutionResults: Boolean =
    getConfSource.getBoolean("web-server.clean-all-execution-results-on-server-start")

  // JDBC configuration
  val jdbcConfig: Config = getConfSource.getConfig("jdbc")

}
