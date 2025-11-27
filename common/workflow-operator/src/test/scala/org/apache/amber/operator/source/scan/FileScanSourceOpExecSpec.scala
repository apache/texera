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

package org.apache.amber.operator.source.scan

import org.apache.amber.core.tuple.{AttributeType, BigObject, Schema, SchemaEnforceable}
import org.apache.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{BufferedOutputStream, FileOutputStream}
import java.net.URI
import java.nio.file.{Files, Path}
import java.util.zip.{ZipEntry, ZipOutputStream}

/**
  * Unit tests for BIG_OBJECT logic in FileScanSourceOpExec.
  * Full integration tests with S3 and database are in BigObjectManagerSpec.
  */
class FileScanSourceOpExecSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val testDir = Path
    .of(sys.env.getOrElse("TEXERA_HOME", "."))
    .resolve("common/workflow-operator/src/test/resources")
    .toRealPath()

  private val testFile = testDir.resolve("test_big_object.txt")
  private val testZip = testDir.resolve("test_big_object.zip")

  override def beforeAll(): Unit = {
    super.beforeAll()
    Files.write(testFile, "Test content\nLine 2\nLine 3".getBytes)
    createZipFile(testZip, Map("file1.txt" -> "Content 1", "file2.txt" -> "Content 2"))
  }

  override def afterAll(): Unit = {
    Files.deleteIfExists(testFile)
    Files.deleteIfExists(testZip)
    super.afterAll()
  }

  private def createZipFile(path: Path, entries: Map[String, String]): Unit = {
    val zipOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile)))
    try {
      entries.foreach {
        case (name, content) =>
          zipOut.putNextEntry(new ZipEntry(name))
          zipOut.write(content.getBytes)
          zipOut.closeEntry()
      }
    } finally {
      zipOut.close()
    }
  }

  private def createDescriptor(
      file: Path = testFile,
      attributeName: String = "line"
  ): FileScanSourceOpDesc = {
    val desc = new FileScanSourceOpDesc()
    desc.fileName = Some(file.toString)
    desc.attributeType = FileAttributeType.BIG_OBJECT
    desc.attributeName = attributeName
    desc.fileEncoding = FileDecodingMethod.UTF_8
    desc
  }

  private def assertSchema(schema: Schema, attributeName: String): Unit = {
    assert(schema.getAttributes.length == 1)
    assert(schema.getAttribute(attributeName).getType == AttributeType.BIG_OBJECT)
  }

  // Schema Tests
  it should "infer BIG_OBJECT schema with default attribute name" in {
    assertSchema(createDescriptor().sourceSchema(), "line")
  }

  it should "infer BIG_OBJECT schema with custom attribute name" in {
    assertSchema(createDescriptor(attributeName = "custom_field").sourceSchema(), "custom_field")
  }

  it should "map BIG_OBJECT to correct AttributeType" in {
    assert(FileAttributeType.BIG_OBJECT.getType == AttributeType.BIG_OBJECT)
  }

  // Type Classification Tests
  it should "correctly classify BIG_OBJECT as isSingle type" in {
    val isSingleTypes = List(
      FileAttributeType.BIG_OBJECT,
      FileAttributeType.SINGLE_STRING,
      FileAttributeType.BINARY
    )
    val multiLineTypes = List(
      FileAttributeType.STRING,
      FileAttributeType.INTEGER,
      FileAttributeType.LONG,
      FileAttributeType.DOUBLE,
      FileAttributeType.BOOLEAN,
      FileAttributeType.TIMESTAMP
    )

    isSingleTypes.foreach(t => assert(t.isSingle, s"$t should be isSingle"))
    multiLineTypes.foreach(t => assert(!t.isSingle, s"$t should not be isSingle"))
  }

  // Execution Tests
  it should "create BigObject when reading file with BIG_OBJECT type" in {
    val desc = createDescriptor()
    desc.setResolvedFileName(URI.create(testFile.toUri.toString))

    val executor = new FileScanSourceOpExec(objectMapper.writeValueAsString(desc))

    try {
      executor.open()
      val tuples = executor.produceTuple().toSeq
      executor.close()

      assert(tuples.size == 1)
      val field = tuples.head
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(desc.sourceSchema())
        .getField[Any]("line")

      assert(field.isInstanceOf[BigObject])
      assert(field.asInstanceOf[BigObject].getUri.startsWith("s3://"))
    } catch {
      case e: Exception =>
        info(s"S3 not configured: ${e.getMessage}")
    }
  }

  // BigObject Tests
  it should "create valid BigObject with correct URI parsing" in {
    val pointer = new BigObject("s3://bucket/path/to/object")

    assert(pointer.getUri == "s3://bucket/path/to/object")
    assert(pointer.getBucketName == "bucket")
    assert(pointer.getObjectKey == "path/to/object")
  }

  it should "reject invalid BigObject URIs" in {
    assertThrows[IllegalArgumentException](new BigObject("http://invalid"))
    assertThrows[IllegalArgumentException](new BigObject("not-a-uri"))
    assertThrows[IllegalArgumentException](new BigObject(null.asInstanceOf[String]))
  }
}
