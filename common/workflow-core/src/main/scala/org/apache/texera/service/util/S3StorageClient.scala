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

package org.apache.texera.service.util

import org.apache.texera.amber.config.StorageConfig
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.core.sync.RequestBody

import java.io.InputStream
import scala.jdk.CollectionConverters._

/**
  * S3Storage provides an abstraction for S3-compatible storage (e.g., MinIO).
  * - Uses credentials and endpoint from StorageConfig.
  * - Supports object upload, download, listing, and deletion.
  */
object S3StorageClient {
  val MINIMUM_NUM_OF_MULTIPART_S3_PART: Long = 5L * 1024 * 1024 // 5 MiB
  val MAXIMUM_NUM_OF_MULTIPART_S3_PARTS = 10_000
  //Keep on sync with LakeFS https://github.com/treeverse/lakeFS/pull/10180
  val PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS = 24
  // S3 DeleteObjects accepts at most 1000 keys per request.
  val MAX_KEYS_PER_DELETE_REQUEST = 1000
  // Cap how many failed keys are listed in the error message.
  private[util] val MAX_LISTED_DELETE_ERRORS = 10

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
    * Deletes every object whose key begins with `directoryPrefix`. S3 keys are a flat namespace
    * with no real directories, so a "directory" is just a shared key prefix.
    *
    * A trailing `/` is added when missing so the prefix matches on a path boundary (`a/b` deletes
    * `a/b/file` but not `a/bc/file`). An empty prefix would match the whole bucket and is rejected.
    *
    * @param bucketName Target S3/MinIO bucket.
    * @param directoryPrefix Non-empty key prefix to delete.
    */
  def deleteDirectory(bucketName: String, directoryPrefix: String): Unit = {
    require(directoryPrefix.nonEmpty, "directoryPrefix must not be empty")
    val prefix = if (directoryPrefix.endsWith("/")) directoryPrefix else directoryPrefix + "/"

    val listRequest = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build()

    // Delete in batches capped at the per-request key limit. Attempt every batch before raising,
    // so one undeletable key can't strand the rest; `quiet(true)` keeps each response to just the
    // failures.
    val errors = s3Client
      .listObjectsV2Paginator(listRequest)
      .contents()
      .asScala
      .iterator
      .map(obj => ObjectIdentifier.builder().key(obj.key()).build())
      .grouped(MAX_KEYS_PER_DELETE_REQUEST)
      .flatMap { batch =>
        s3Client
          .deleteObjects(
            DeleteObjectsRequest
              .builder()
              .bucket(bucketName)
              .delete(Delete.builder().objects(batch.asJava).quiet(true).build())
              .build()
          )
          .errors()
          .asScala
      }
      .toList

    throwOnDeleteErrors(prefix, errors)
  }

  /** Raise if any object failed to delete, listing up to `MAX_LISTED_DELETE_ERRORS` keys. */
  private[util] def throwOnDeleteErrors(prefix: String, errors: Seq[S3Error]): Unit = {
    if (errors.nonEmpty) {
      val listed = errors.take(MAX_LISTED_DELETE_ERRORS).map(e => s"${e.key()} (${e.code()})")
      val summary =
        if (errors.size > MAX_LISTED_DELETE_ERRORS)
          s" (and ${errors.size - MAX_LISTED_DELETE_ERRORS} more)"
        else ""
      throw new RuntimeException(
        s"Failed to delete ${errors.size} object(s) under prefix '$prefix': " +
          listed.mkString(", ") + summary
      )
    }
  }

  /**
    * Uploads an object to S3 using multipart upload.
    * Handles streams of any size without loading into memory.
    */
  def uploadObject(bucketName: String, objectKey: String, inputStream: InputStream): String = {
    val buffer = new Array[Byte](MINIMUM_NUM_OF_MULTIPART_S3_PART.toInt)

    // Helper to read a full buffer from input stream
    def readChunk(): Int = {
      var offset = 0
      var read = 0
      while (
        offset < buffer.length && {
          read = inputStream.read(buffer, offset, buffer.length - offset); read > 0
        }
      ) {
        offset += read
      }
      offset
    }

    // Read first chunk to check if stream is empty
    val firstChunkSize = readChunk()
    if (firstChunkSize == 0) {
      return s3Client
        .putObject(
          PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
          RequestBody.fromBytes(Array.empty[Byte])
        )
        .eTag()
    }

    val uploadId = s3Client
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(objectKey).build()
      )
      .uploadId()

    var uploadSuccess = false
    try {
      // Upload all parts using an iterator
      val allParts = Iterator
        .iterate((1, firstChunkSize)) { case (partNum, _) => (partNum + 1, readChunk()) }
        .takeWhile { case (_, size) => size > 0 }
        .map {
          case (partNumber, chunkSize) =>
            val eTag = s3Client
              .uploadPart(
                UploadPartRequest
                  .builder()
                  .bucket(bucketName)
                  .key(objectKey)
                  .uploadId(uploadId)
                  .partNumber(partNumber)
                  .build(),
                RequestBody.fromBytes(buffer.take(chunkSize))
              )
              .eTag()
            CompletedPart.builder().partNumber(partNumber).eTag(eTag).build()
        }
        .toList

      val result = s3Client
        .completeMultipartUpload(
          CompleteMultipartUploadRequest
            .builder()
            .bucket(bucketName)
            .key(objectKey)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(allParts.asJava).build())
            .build()
        )
        .eTag()

      uploadSuccess = true
      result

    } finally {
      if (!uploadSuccess) {
        try {
          s3Client.abortMultipartUpload(
            AbortMultipartUploadRequest
              .builder()
              .bucket(bucketName)
              .key(objectKey)
              .uploadId(uploadId)
              .build()
          )
        } catch { case _: Exception => }
      }
    }
  }

  /**
    * Downloads an object from S3 as an InputStream.
    *
    * @param bucketName The S3 bucket name.
    * @param objectKey The object key (path) in S3.
    * @return An InputStream containing the object data.
    */
  def downloadObject(bucketName: String, objectKey: String): InputStream = {
    s3Client.getObject(
      GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
    )
  }

  /**
    * Deletes a single object from S3.
    *
    * @param bucketName The S3 bucket name.
    * @param objectKey The object key (path) in S3.
    */
  def deleteObject(bucketName: String, objectKey: String): Unit = {
    s3Client.deleteObject(
      DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build()
    )
  }

  /**
    * Uploads a single part for an in-progress S3 multipart upload.
    *
    * This method wraps the AWS SDK v2 {@code UploadPart} API:
    * it builds an [[software.amazon.awssdk.services.s3.model.UploadPartRequest]]
    * and streams the part payload via a [[software.amazon.awssdk.core.sync.RequestBody]].
    *
    * Payload handling:
    *   - If {@code contentLength} is provided, the payload is streamed directly from {@code inputStream}
    *     using {@code RequestBody.fromInputStream(inputStream, len)}.
    *   - If {@code contentLength} is {@code None}, the entire {@code inputStream} is read into memory
    *     ({@code readAllBytes}) and uploaded using {@code RequestBody.fromBytes(bytes)}.
    *     This is convenient but can be memory-expensive for large parts; prefer providing a known length.
    *
    * Notes:
    *   - {@code partNumber} must be in the valid S3 range (typically 1..10,000).
    *   - The caller is responsible for closing {@code inputStream}.
    *   - This method is synchronous and will block the calling thread until the upload completes.
    *
    * @param bucket        S3 bucket name.
    * @param key           Object key (path) being uploaded.
    * @param uploadId      Multipart upload identifier returned by CreateMultipartUpload.
    * @param partNumber    1-based part number for this upload.
    * @param inputStream   Stream containing the bytes for this part.
    * @param contentLength Optional size (in bytes) of this part; provide it to avoid buffering in memory.
    * @return              The [[software.amazon.awssdk.services.s3.model.UploadPartResponse]],
    *                      including the part ETag used for completing the multipart upload.
    */
  def uploadPartWithRequest(
      bucket: String,
      key: String,
      uploadId: String,
      partNumber: Int,
      inputStream: InputStream,
      contentLength: Option[Long]
  ): UploadPartResponse = {
    val requestBody: RequestBody = contentLength match {
      case Some(len) => RequestBody.fromInputStream(inputStream, len)
      case None =>
        val bytes = inputStream.readAllBytes()
        RequestBody.fromBytes(bytes)
    }

    val req = UploadPartRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .partNumber(partNumber)
      .build()

    s3Client.uploadPart(req, requestBody)
  }

}
