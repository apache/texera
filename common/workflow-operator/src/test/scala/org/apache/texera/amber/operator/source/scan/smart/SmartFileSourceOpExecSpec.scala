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

package org.apache.texera.amber.operator.source.scan.smart

import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters._

class SmartFileSourceOpExecSpec extends AnyFlatSpec {

  "SmartFileSourceOpExec" should "read a folder of similar CSV files as one source" in {
    val dir = Files.createTempDirectory("smartfile-folder-exec-")
    try {
      Files.writeString(dir.resolve("2025-01.csv"), "id,name\n1,Ada\n", StandardCharsets.UTF_8)
      Files.writeString(dir.resolve("2025-02.csv"), "id,name\n2,Lin\n", StandardCharsets.UTF_8)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val exec = new SmartFileSourceOpExec(objectMapper.writeValueAsString(desc))
      exec.open()
      val tuples = exec.produceTuple().toList
      exec.close()

      assert(tuples.size == 2)
      assert(tuples.map(_.getFields(0)) == List(1, 2))
      assert(tuples.map(_.getFields(1)) == List("Ada", "Lin"))
    } finally deleteRecursively(dir)
  }

  it should "preserve the originating file for folder rows when enabled" in {
    val dir = Files.createTempDirectory("smartfile-folder-source-column-exec-")
    try {
      Files.writeString(dir.resolve("2025-01.csv"), "id,name\n1,Ada\n", StandardCharsets.UTF_8)
      Files.writeString(dir.resolve("2025-02.csv"), "id,name\n2,Lin\n", StandardCharsets.UTF_8)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))
      desc.includeSourceFile = true

      val exec = new SmartFileSourceOpExec(objectMapper.writeValueAsString(desc))
      exec.open()
      val tuples = exec.produceTuple().toList
      exec.close()

      assert(tuples.map(_.getFields.last) == List("2025-01.csv", "2025-02.csv"))
    } finally deleteRecursively(dir)
  }

  it should "read image folders as image records with metadata" in {
    val dir = Files.createTempDirectory("smartfile-image-folder-exec-")
    try {
      writePng(dir.resolve("cat.png").toFile, width = 3, height = 2)
      writePng(dir.resolve("dog.png").toFile, width = 4, height = 5)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))
      desc.includeSourceFile = true

      val exec = new SmartFileSourceOpExec(objectMapper.writeValueAsString(desc))
      exec.open()
      val tuples = exec.produceTuple().toList
      exec.close()

      assert(tuples.size == 2)
      assert(tuples.map(_.getFields(0).asInstanceOf[Array[Byte]].nonEmpty) == List(true, true))
      assert(tuples.map(_.getFields(1)) == List("png", "png"))
      assert(tuples.map(_.getFields(2)) == List(3, 4))
      assert(tuples.map(_.getFields(3)) == List(2, 5))
      assert(tuples.map(_.getFields(4)) == List("cat.png", "dog.png"))
    } finally deleteRecursively(dir)
  }

  private def deleteRecursively(path: java.nio.file.Path): Unit = {
    Files
      .walk(path)
      .iterator()
      .asScala
      .toSeq
      .sortBy(_.getNameCount)(Ordering.Int.reverse)
      .foreach(Files.deleteIfExists)
  }

  private def writePng(out: File, width: Int, height: Int): Unit = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    ImageIO.write(image, "png", out)
  }
}
