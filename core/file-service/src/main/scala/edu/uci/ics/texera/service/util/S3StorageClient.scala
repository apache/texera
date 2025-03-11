package edu.uci.ics.texera.service.util

import edu.uci.ics.amber.core.storage.StorageConfig
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model._

import java.security.MessageDigest
import scala.jdk.CollectionConverters._

/**
  * S3Storage provides an abstraction for S3-compatible storage (e.g., MinIO).
  * - Uses credentials and endpoint from StorageConfig.
  * - Supports object upload, download, listing, and deletion.
  */
object S3StorageClient {
  // Initialize MinIO-compatible S3 Client
  private lazy val s3Client: S3Client = {
    val credentials = AwsBasicCredentials.create(StorageConfig.s3Username, StorageConfig.s3Password)
    S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .region(Region.of(StorageConfig.s3Region))
      .endpointOverride(java.net.URI.create(StorageConfig.s3Endpoint)) // MinIO URL
      .serviceConfiguration(
        S3Configuration.builder().pathStyleAccessEnabled(true).build()
      )
      .build()
  }

  /**
    * Checks if a directory (prefix) exists within an S3 bucket.
    *
    * @param bucketName The bucket name.
    * @param directoryPrefix The directory (prefix) to check (must end with `/`).
    * @return True if the directory contains at least one object, False otherwise.
    */
  def directoryExists(bucketName: String, directoryPrefix: String): Boolean = {
    // Ensure the prefix ends with `/` to correctly match directories
    val normalizedPrefix =
      if (directoryPrefix.endsWith("/")) directoryPrefix else directoryPrefix + "/"

    val listRequest = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .prefix(normalizedPrefix)
      .maxKeys(1) // Only check if at least one object exists
      .build()

    val listResponse = s3Client.listObjectsV2(listRequest)
    !listResponse.contents().isEmpty // If contents exist, directory exists
  }

  /**
    * Creates an S3 bucket if it does not already exist.
    *
    * @param bucketName The name of the bucket to create.
    */
  def createBucketIfNotExist(bucketName: String): Unit = {
    try {
      // Check if the bucket already exists
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
    } catch {
      case _: NoSuchBucketException | _: S3Exception =>
        // If the bucket does not exist, create it
        val createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build()
        s3Client.createBucket(createBucketRequest)
        println(s"Bucket '$bucketName' created successfully.")
    }
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
