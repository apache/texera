package edu.uci.ics.amber.core.storage

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.presigner.PresignedRequest
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model._

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.time.Duration
import java.util.Base64
import scala.jdk.CollectionConverters._

/**
  * S3Storage provides an abstraction for S3-compatible storage (e.g., MinIO).
  * - Uses credentials and endpoint from StorageConfig.
  * - Supports object upload, download, listing, and deletion.
  */
object S3Storage {
  // Initialize MinIO-compatible S3 Client
  private lazy val s3Client: S3Client = {
    val credentials = AwsBasicCredentials.create(StorageConfig.s3Username, StorageConfig.s3Password)
    S3Client
      .builder()
      .region(Region.US_WEST_2) // MinIO doesn't require region, but AWS SDK enforces one
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .endpointOverride(java.net.URI.create(StorageConfig.s3Endpoint)) // MinIO URL
      .serviceConfiguration(
        S3Configuration.builder().pathStyleAccessEnabled(true).build()
      ) // Needed for MinIO
      .build()
  }

  /**
    * Deletes a directory (all objects under a given prefix) from a bucket.
    *
    * @param bucketName Target S3/MinIO bucket.
    * @param directoryPrefix The directory to delete (must end with `/`).
    */
  def deleteDirectory(bucketName: String, directoryPrefix: String): Unit = {
    // Ensure the directory prefix ends with `/` to avoid accidental deletions
    val prefix = if (directoryPrefix.endsWith("/")) directoryPrefix else directoryPrefix + "/"

    // List objects under the given prefix
    val listRequest = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .prefix(prefix)
      .build()

    val listResponse = s3Client.listObjectsV2(listRequest)

    // Extract object keys
    val objectKeys = listResponse.contents().asScala.map(_.key())

    if (objectKeys.nonEmpty) {
      val objectsToDelete =
        objectKeys.map(key => ObjectIdentifier.builder().key(key).build()).asJava

      val deleteRequest = Delete
        .builder()
        .objects(objectsToDelete)
        .build()

      // Compute MD5 checksum for MinIO if required
      val md5Hash = MessageDigest
        .getInstance("MD5")
        .digest(deleteRequest.toString.getBytes("UTF-8"))

      val contentMD5 = Base64.getEncoder.encodeToString(md5Hash)

      // Convert object keys to S3 DeleteObjectsRequest format
      val deleteObjectsRequest = DeleteObjectsRequest
        .builder()
        .bucket(bucketName)
        .delete(deleteRequest)
        .build()

      // Perform batch deletion
      s3Client.deleteObjects(deleteObjectsRequest)
    }
  }
}
