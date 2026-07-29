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

import org.apache.texera.common.config.StorageConfig
import org.apache.texera.common.tags.NonParallelTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random

@NonParallelTest
class S3StorageClientSpec
    extends AnyFunSuite
    with S3StorageTestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testBucketName = "test-s3-storage-client"

  override def beforeAll(): Unit = {
    super.beforeAll()
    S3StorageClient.createBucketIfNotExist(testBucketName)
  }

  override def afterAll(): Unit = {
    // Best-effort cleanup of the prefixes these tests use (deleteDirectory rejects an empty prefix).
    try {
      Seq("test", "delete-dir", "multipart")
        .foreach(S3StorageClient.deleteDirectory(testBucketName, _))
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    try rawClient.close()
    catch { case _: Exception => }
    super.afterAll()
  }

  /**
    * A second S3 client pointed at the same MinIO container. S3StorageClient exposes only
    * `uploadPartWithRequest` from the multipart API, so the surrounding create/complete/list
    * calls are issued directly instead of being mocked away.
    */
  private lazy val rawClient: S3Client = {
    val credentials = AwsBasicCredentials.create(StorageConfig.s3Username, StorageConfig.s3Password)
    S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .region(Region.of(StorageConfig.s3Region))
      .endpointOverride(java.net.URI.create(StorageConfig.s3Endpoint))
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .build()
  }

  private def startMultipartUpload(objectKey: String): String =
    rawClient
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(testBucketName).key(objectKey).build()
      )
      .uploadId()

  private def completeMultipartUpload(
      objectKey: String,
      uploadId: String,
      parts: Seq[CompletedPart]
  ): Unit =
    rawClient.completeMultipartUpload(
      CompleteMultipartUploadRequest
        .builder()
        .bucket(testBucketName)
        .key(objectKey)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(parts.asJava).build())
        .build()
    )

  /** Upload ids still pending (i.e. neither completed nor aborted) for the given key. */
  private def pendingUploadIds(objectKey: String): Seq[String] =
    rawClient
      .listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(testBucketName).build())
      .uploads()
      .asScala
      .filter(_.key() == objectKey)
      .map(_.uploadId())
      .toSeq

  // Helper methods
  private def createInputStream(data: String): ByteArrayInputStream = {
    new ByteArrayInputStream(data.getBytes)
  }

  private def createInputStream(data: Array[Byte]): ByteArrayInputStream = {
    new ByteArrayInputStream(data)
  }

  private def readInputStream(inputStream: java.io.InputStream): Array[Byte] = {
    val buffer = new Array[Byte](8192)
    val outputStream = new java.io.ByteArrayOutputStream()
    var bytesRead = 0
    while ({
      bytesRead = inputStream.read(buffer); bytesRead != -1
    }) {
      outputStream.write(buffer, 0, bytesRead)
    }
    outputStream.toByteArray
  }

  // ========================================
  // uploadObject Tests
  // ========================================

  test("uploadObject should upload a small object successfully") {
    val testData = "Hello, World! This is a small test object."
    val objectKey = "test/small-object.txt"

    val eTag = S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(testData))

    assert(eTag != null)
    assert(eTag.nonEmpty)

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadObject should upload an empty object") {
    val objectKey = "test/empty-object.txt"

    val eTag = S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(""))

    assert(eTag != null)

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadObject should upload a large object using multipart upload") {
    // Create data larger than MINIMUM_NUM_OF_MULTIPART_S3_PART (5MB)
    val largeData = Array.fill[Byte](6 * 1024 * 1024)((Random.nextInt(256) - 128).toByte)
    val objectKey = "test/large-object.bin"

    val eTag = S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(largeData))

    assert(eTag != null)
    assert(eTag.nonEmpty)

    // Verify the uploaded content
    val downloadedStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = readInputStream(downloadedStream)
    downloadedStream.close()

    assert(downloadedData.length == largeData.length)
    assert(downloadedData.sameElements(largeData))

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadObject should handle objects with special characters in key") {
    val testData = "Testing special characters"
    val objectKey = "test/special-chars/file with spaces & symbols!@#.txt"

    val eTag = S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(testData))

    assert(eTag != null)

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadObject should overwrite existing object") {
    val objectKey = "test/overwrite-test.txt"
    val data1 = "Original data"
    val data2 = "Updated data"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(data1))
    val eTag2 = S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(data2))

    assert(eTag2 != null)

    val downloadedStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = new String(readInputStream(downloadedStream))
    downloadedStream.close()

    assert(downloadedData == data2)

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  // ========================================
  // downloadObject Tests
  // ========================================

  test("downloadObject should download an object successfully") {
    val testData = "This is test data for download."
    val objectKey = "test/download-test.txt"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(testData))

    val inputStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = new String(readInputStream(inputStream))
    inputStream.close()

    assert(downloadedData == testData)

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("downloadObject should download large objects correctly") {
    val largeData = Array.fill[Byte](10 * 1024 * 1024)((Random.nextInt(256) - 128).toByte)
    val objectKey = "test/large-download-test.bin"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(largeData))

    val inputStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = readInputStream(inputStream)
    inputStream.close()

    assert(downloadedData.length == largeData.length)
    assert(downloadedData.sameElements(largeData))

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("downloadObject should download empty objects") {
    val objectKey = "test/empty-download-test.txt"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(""))

    val inputStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = readInputStream(inputStream)
    inputStream.close()

    assert(downloadedData.isEmpty)

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("downloadObject should throw exception for non-existent object") {
    val nonExistentKey = "test/non-existent-object.txt"

    assertThrows[Exception] {
      S3StorageClient.downloadObject(testBucketName, nonExistentKey)
    }
  }

  test("downloadObject should handle binary data correctly") {
    val binaryData = Array[Byte](0, 1, 2, 127, -128, -1, 64, 32, 16, 8, 4, 2, 1)
    val objectKey = "test/binary-data.bin"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(binaryData))

    val inputStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = readInputStream(inputStream)
    inputStream.close()

    assert(downloadedData.sameElements(binaryData))

    // Clean up
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  // ========================================
  // deleteObject Tests
  // ========================================

  test("deleteObject should delete an existing object") {
    val objectKey = "test/delete-test.txt"
    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream("delete me"))

    S3StorageClient.deleteObject(testBucketName, objectKey)

    // Verify deletion by attempting to download
    assertThrows[Exception] {
      S3StorageClient.downloadObject(testBucketName, objectKey)
    }
  }

  test("deleteObject should not throw exception for non-existent object") {
    val nonExistentKey = "test/already-deleted.txt"

    // Should not throw exception
    S3StorageClient.deleteObject(testBucketName, nonExistentKey)
  }

  test("deleteObject should delete large objects") {
    val largeData = Array.fill[Byte](7 * 1024 * 1024)((Random.nextInt(256) - 128).toByte)
    val objectKey = "test/large-delete-test.bin"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(largeData))

    S3StorageClient.deleteObject(testBucketName, objectKey)

    // Verify deletion by attempting to download
    assertThrows[Exception] {
      S3StorageClient.downloadObject(testBucketName, objectKey)
    }
  }

  test("deleteObject should handle multiple deletions of the same object") {
    val objectKey = "test/multi-delete-test.txt"
    S3StorageClient.uploadObject(
      testBucketName,
      objectKey,
      createInputStream("delete multiple times")
    )

    S3StorageClient.deleteObject(testBucketName, objectKey)

    // Second delete should not throw exception
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  // ========================================
  // Integration Tests (combining methods)
  // ========================================

  test("upload, download, and delete workflow should work correctly") {
    val testData = "Complete workflow test data"
    val objectKey = "test/workflow-test.txt"

    // Upload
    val eTag = S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(testData))
    assert(eTag != null)

    // Download
    val inputStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = new String(readInputStream(inputStream))
    inputStream.close()
    assert(downloadedData == testData)

    // Delete
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("multiple objects can be managed independently") {
    val objects = Map(
      "test/object1.txt" -> "Data for object 1",
      "test/object2.txt" -> "Data for object 2",
      "test/object3.txt" -> "Data for object 3"
    )

    // Upload all objects
    objects.foreach {
      case (key, data) =>
        S3StorageClient.uploadObject(testBucketName, key, createInputStream(data))
    }

    // Delete one object
    S3StorageClient.deleteObject(testBucketName, "test/object2.txt")

    // Clean up remaining objects
    S3StorageClient.deleteObject(testBucketName, "test/object1.txt")
    S3StorageClient.deleteObject(testBucketName, "test/object3.txt")
  }

  test("objects with nested paths should be handled correctly") {
    val objectKey = "test/deeply/nested/path/to/object.txt"
    val testData = "Nested path test"

    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream(testData))

    val inputStream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloadedData = new String(readInputStream(inputStream))
    inputStream.close()
    assert(downloadedData == testData)

    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  // ========================================
  // deleteDirectory Tests
  // ========================================

  test("deleteDirectory should delete all objects under a prefix") {
    val prefix = "delete-dir/small"
    val keys = (0 until 5).map(i => s"$prefix/object-$i.txt")
    keys.foreach(key =>
      S3StorageClient.uploadObject(testBucketName, key, createInputStream("data"))
    )

    assert(S3StorageClient.directoryExists(testBucketName, prefix))

    S3StorageClient.deleteDirectory(testBucketName, prefix)

    assert(!S3StorageClient.directoryExists(testBucketName, prefix))
  }

  test("deleteDirectory should not delete siblings that merely share the prefix string") {
    // The trailing-slash guard: deleting "delete-dir/small" (→ "delete-dir/small/") must leave the
    // sibling "delete-dir/small-sibling.txt" untouched.
    val prefix = "delete-dir/small"
    S3StorageClient.uploadObject(testBucketName, s"$prefix/object.txt", createInputStream("data"))
    val sibling = "delete-dir/small-sibling.txt"
    S3StorageClient.uploadObject(testBucketName, sibling, createInputStream("keep me"))

    S3StorageClient.deleteDirectory(testBucketName, prefix)

    assert(!S3StorageClient.directoryExists(testBucketName, prefix))
    val survivor = S3StorageClient.downloadObject(testBucketName, sibling)
    assert(new String(readInputStream(survivor)) == "keep me")
    survivor.close()
    S3StorageClient.deleteObject(testBucketName, sibling)
  }

  test("deleteDirectory should delete more than 1000 objects under a prefix") {
    // >1000 objects exercises pagination and delete batching; without them the tail is orphaned.
    val prefix = "delete-dir/large"
    val objectCount = 1001

    // Upload with bounded concurrency to keep the test reasonably fast without flooding the
    // shared MinIO container (a 16-way burst was a contributor to the flakiness in issue #7049).
    val pool = Executors.newFixedThreadPool(4)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      val uploads = (0 until objectCount).map { i =>
        Future {
          S3StorageClient.uploadObject(
            testBucketName,
            f"$prefix/object-$i%05d.txt",
            createInputStream("")
          )
        }
      }
      Await.result(Future.sequence(uploads), 5.minutes)
    } finally {
      pool.shutdown()
    }

    assert(S3StorageClient.directoryExists(testBucketName, prefix))

    S3StorageClient.deleteDirectory(testBucketName, prefix)

    assert(!S3StorageClient.directoryExists(testBucketName, prefix))
  }

  test("deleteDirectory should not throw for a prefix with no objects") {
    // Empty listing: no DeleteObjects request is issued.
    S3StorageClient.deleteDirectory(testBucketName, "delete-dir/non-existent")
  }

  // A real per-key failure needs object-lock setup, so test throwOnDeleteErrors directly.

  test("throwOnDeleteErrors should raise on a per-key delete failure") {
    val errors = Seq(S3Error.builder().key("delete-dir/locked.txt").code("AccessDenied").build())
    val thrown = intercept[RuntimeException] {
      S3StorageClient.throwOnDeleteErrors("delete-dir/", errors)
    }
    assert(thrown.getMessage.contains("delete-dir/locked.txt"))
    assert(thrown.getMessage.contains("AccessDenied"))
  }

  test("throwOnDeleteErrors should not throw when there are no errors") {
    S3StorageClient.throwOnDeleteErrors("delete-dir/", Seq.empty[S3Error])
  }

  test("throwOnDeleteErrors should report the true total but list at most the cap") {
    val cap = S3StorageClient.MAX_LISTED_DELETE_ERRORS
    val errorCount = cap + 5
    val errors = (0 until errorCount).map(i =>
      S3Error.builder().key(f"delete-dir/locked-$i%02d.txt").code("AccessDenied").build()
    )
    val thrown = intercept[RuntimeException] {
      S3StorageClient.throwOnDeleteErrors("delete-dir/", errors)
    }
    assert(thrown.getMessage.contains(s"$errorCount object(s)"))
    assert(thrown.getMessage.contains("delete-dir/locked-00.txt")) // first key is listed
    assert(!thrown.getMessage.contains(f"delete-dir/locked-$cap%02d.txt")) // capped key is not
    assert(thrown.getMessage.contains(s"and ${errorCount - cap} more"))
  }

  test("deleteDirectory should reject an empty prefix instead of wiping the bucket") {
    val guard = "delete-dir/guarded-object.txt"
    S3StorageClient.uploadObject(testBucketName, guard, createInputStream("keep me"))

    val thrown = intercept[IllegalArgumentException] {
      S3StorageClient.deleteDirectory(testBucketName, "")
    }
    assert(thrown.getMessage.contains("directoryPrefix must not be empty"))

    // The bucket is untouched.
    val survivor = S3StorageClient.downloadObject(testBucketName, guard)
    assert(new String(readInputStream(survivor)) == "keep me")
    survivor.close()
    S3StorageClient.deleteObject(testBucketName, guard)
  }

  // ========================================
  // directoryExists Tests
  // ========================================

  test("directoryExists should treat a prefix with and without a trailing slash the same") {
    val prefix = "test/exists-dir"
    S3StorageClient.uploadObject(
      testBucketName,
      s"$prefix/object.txt",
      createInputStream("data")
    )

    assert(S3StorageClient.directoryExists(testBucketName, prefix))
    assert(S3StorageClient.directoryExists(testBucketName, s"$prefix/"))

    S3StorageClient.deleteObject(testBucketName, s"$prefix/object.txt")
  }

  test("directoryExists should be false for a prefix that only matches an object name") {
    // "test/exists-file" names an object, not a directory, so no key starts with
    // "test/exists-file/".
    val objectKey = "test/exists-file"
    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream("data"))

    assert(!S3StorageClient.directoryExists(testBucketName, objectKey))

    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("directoryExists should be false for an unused prefix") {
    assert(!S3StorageClient.directoryExists(testBucketName, "test/never-written"))
  }

  // ========================================
  // createBucketIfNotExist Tests
  // ========================================

  test("createBucketIfNotExist should be idempotent for an existing bucket") {
    // beforeAll already created it; a second call takes the headBucket-succeeds path.
    S3StorageClient.createBucketIfNotExist(testBucketName)
    S3StorageClient.createBucketIfNotExist(testBucketName)

    // The bucket is still usable afterwards.
    val objectKey = "test/after-recreate.txt"
    S3StorageClient.uploadObject(testBucketName, objectKey, createInputStream("still here"))
    val stream = S3StorageClient.downloadObject(testBucketName, objectKey)
    assert(new String(readInputStream(stream)) == "still here")
    stream.close()
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  // ========================================
  // uploadPartWithRequest Tests
  // ========================================

  test("uploadPartWithRequest should stream a part when the content length is known") {
    val objectKey = "multipart/known-length.bin"
    val partData = "part payload with a known length".getBytes
    val uploadId = startMultipartUpload(objectKey)

    val response = S3StorageClient.uploadPartWithRequest(
      testBucketName,
      objectKey,
      uploadId,
      partNumber = 1,
      createInputStream(partData),
      contentLength = Some(partData.length.toLong)
    )

    assert(response.eTag() != null && response.eTag().nonEmpty)
    completeMultipartUpload(
      objectKey,
      uploadId,
      Seq(CompletedPart.builder().partNumber(1).eTag(response.eTag()).build())
    )

    val stream = S3StorageClient.downloadObject(testBucketName, objectKey)
    assert(readInputStream(stream).sameElements(partData))
    stream.close()
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadPartWithRequest should buffer the whole stream when no content length is given") {
    val objectKey = "multipart/unknown-length.bin"
    val partData = "part payload with no declared length".getBytes
    val uploadId = startMultipartUpload(objectKey)

    val response = S3StorageClient.uploadPartWithRequest(
      testBucketName,
      objectKey,
      uploadId,
      partNumber = 1,
      createInputStream(partData),
      contentLength = None
    )

    completeMultipartUpload(
      objectKey,
      uploadId,
      Seq(CompletedPart.builder().partNumber(1).eTag(response.eTag()).build())
    )

    val stream = S3StorageClient.downloadObject(testBucketName, objectKey)
    assert(readInputStream(stream).sameElements(partData))
    stream.close()
    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadPartWithRequest should assemble parts in part-number order") {
    // Two parts: every part but the last must be at least 5 MiB, so the head is sized exactly
    // at the multipart minimum and the tail is short.
    val objectKey = "multipart/ordered-parts.bin"
    val head = Array.fill[Byte](S3StorageClient.MINIMUM_NUM_OF_MULTIPART_S3_PART.toInt)(1.toByte)
    val tail = Array.fill[Byte](1024)(2.toByte)
    val uploadId = startMultipartUpload(objectKey)

    val firstETag = S3StorageClient
      .uploadPartWithRequest(
        testBucketName,
        objectKey,
        uploadId,
        partNumber = 1,
        createInputStream(head),
        contentLength = Some(head.length.toLong)
      )
      .eTag()
    val secondETag = S3StorageClient
      .uploadPartWithRequest(
        testBucketName,
        objectKey,
        uploadId,
        partNumber = 2,
        createInputStream(tail),
        contentLength = None
      )
      .eTag()

    completeMultipartUpload(
      objectKey,
      uploadId,
      Seq(
        CompletedPart.builder().partNumber(1).eTag(firstETag).build(),
        CompletedPart.builder().partNumber(2).eTag(secondETag).build()
      )
    )

    val stream = S3StorageClient.downloadObject(testBucketName, objectKey)
    val downloaded = readInputStream(stream)
    stream.close()

    assert(downloaded.length == head.length + tail.length)
    assert(downloaded.take(head.length).sameElements(head))
    assert(downloaded.drop(head.length).sameElements(tail))

    S3StorageClient.deleteObject(testBucketName, objectKey)
  }

  test("uploadPartWithRequest should fail for an unknown upload id") {
    val objectKey = "multipart/no-such-upload.bin"
    val thrown = intercept[S3Exception] {
      S3StorageClient.uploadPartWithRequest(
        testBucketName,
        objectKey,
        uploadId = "not-a-real-upload-id",
        partNumber = 1,
        createInputStream("payload"),
        contentLength = None
      )
    }
    assert(thrown.statusCode() == 404)
  }

  // ========================================
  // uploadObject failure-path Tests
  // ========================================

  test("uploadObject should abort the multipart upload when the source stream fails") {
    // Fails only after the first full part has been read, so a multipart upload is already
    // in flight when the error surfaces.
    val objectKey = "multipart/failing-source.bin"
    val firstPart =
      Array.fill[Byte](S3StorageClient.MINIMUM_NUM_OF_MULTIPART_S3_PART.toInt)(7.toByte)

    val failingStream = new InputStream {
      private val delegate = new ByteArrayInputStream(firstPart)
      override def read(): Int = throw new IOException("stream broke")
      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        val n = delegate.read(b, off, len)
        if (n <= 0) throw new IOException("stream broke") else n
      }
    }

    val thrown = intercept[IOException] {
      S3StorageClient.uploadObject(testBucketName, objectKey, failingStream)
    }
    assert(thrown.getMessage == "stream broke")

    // The in-flight upload was aborted, so no orphaned upload is left behind...
    assert(pendingUploadIds(objectKey).isEmpty)
    // ...and no object was published under the key.
    val missing = intercept[S3Exception] {
      S3StorageClient.downloadObject(testBucketName, objectKey)
    }
    assert(missing.statusCode() == 404)
  }
}
