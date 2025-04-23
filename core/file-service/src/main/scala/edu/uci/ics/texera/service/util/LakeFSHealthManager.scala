package edu.uci.ics.texera.service.util

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.slf4j.LoggerFactory

class LakeFSHealthManager(intervalSeconds: Int) extends io.dropwizard.lifecycle.Managed {
  private val logger = LoggerFactory.getLogger(getClass)
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  @volatile private var bucketCreated = false

  override def start(): Unit = {
    scheduler.scheduleAtFixedRate(
      () => {
        if (!bucketCreated) {
          try {
            S3StorageClient.createBucketIfNotExist(StorageConfig.lakefsBucketName)
            logger.info("S3 bucket created successfully.")
            bucketCreated = true
          } catch {
            case e: Exception =>
              logger.warn("Periodic bucket creation failed", e)
          }
        }

        try {
          LakeFSStorageClient.healthCheck()
        } catch {
          case e: Exception =>
            logger.warn("LakeFS health check failed", e)
        }
      },
      0,
      intervalSeconds,
      TimeUnit.SECONDS
    )
  }

  override def stop(): Unit = {
    scheduler.shutdownNow()
  }
}
