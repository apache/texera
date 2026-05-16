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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.schema.{MessageTypeParser, Type}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.TestOperators
import org.scalatest.flatspec.AnyFlatSpec

import java.awt.image.BufferedImage
import java.io.{File, FileOutputStream}
import javax.imageio.ImageIO
import java.nio.file.Files
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class SmartFileSourceOpDescSpec extends AnyFlatSpec {

  "SmartFileSourceOpDesc.operatorInfo" should "advertise the broader Smart Source name" in {
    val desc = new SmartFileSourceOpDesc()

    assert(desc.operatorInfo.userFriendlyName == "Smart Source")
  }

  "SmartFileSourceOpDesc" should "infer CSV format and schema from a CSV file" in {
    val desc = new SmartFileSourceOpDesc()
    desc.fileName = Some(TestOperators.CountrySalesSmallCsvPath)
    desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

    val result = desc.runInference()
    assert(result.format == SmartFileFormat.CSV)
    assert(result.csvDelimiter.contains(","))
    assert(result.csvHasHeader.contains(true))
    assert(result.schema.getAttributes.length == 14)
    assert(result.schema.getAttribute("Order ID").getType == AttributeType.INTEGER)
  }

  it should "infer JSONL format and schema from a JSONL file" in {
    val desc = new SmartFileSourceOpDesc()
    desc.fileName = Some(TestOperators.smallJsonLPath)
    desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

    val result = desc.runInference()
    assert(result.format == SmartFileFormat.JSONL)
    assert(result.schema.getAttributes.nonEmpty)
  }

  it should "respect a formatOverride from the user" in {
    val desc = new SmartFileSourceOpDesc()
    desc.fileName = Some(TestOperators.CountrySalesSmallCsvPath)
    desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))
    desc.formatOverride = SmartFileFormat.CSV
    desc.customDelimiter = Some(",")

    val result = desc.runInference()
    assert(result.format == SmartFileFormat.CSV)
  }

  it should "infer plain text format for a .txt file" in {
    val desc = new SmartFileSourceOpDesc()
    desc.fileName = Some(TestOperators.TestTextFilePath)
    desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

    val result = desc.runInference()
    assert(result.format == SmartFileFormat.TEXT)
    assert(result.schema.getAttributeNames == List("line"))
    assert(result.schema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer string columns for a header-only CSV file" in {
    val tmp = Files.createTempFile("smartfile-header-only-", ".csv")
    try {
      Files.writeString(tmp, "id,name,score\n", StandardCharsets.UTF_8)
      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(tmp.toFile.getAbsolutePath)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val result = desc.runInference()
      assert(result.format == SmartFileFormat.CSV)
      assert(result.schema.getAttributeNames == List("id", "name", "score"))
      assert(result.schema.getAttributes.forall(_.getType == AttributeType.STRING))
    } finally Files.deleteIfExists(tmp)
  }

  it should "infer one schema for a folder of similar CSV files" in {
    val dir = Files.createTempDirectory("smartfile-folder-")
    try {
      Files.writeString(dir.resolve("2025-01.csv"), "id,name\n1,Ada\n", StandardCharsets.UTF_8)
      Files.writeString(dir.resolve("2025-02.csv"), "id,name\n2,Lin\n", StandardCharsets.UTF_8)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val result = desc.runInference()
      assert(result.format == SmartFileFormat.CSV)
      assert(result.isFolder)
      assert(result.fileCount == 2)
      assert(result.schema.getAttributeNames == List("id", "name"))
    } finally deleteRecursively(dir)
  }

  it should "infer image folders as image records" in {
    val dir = Files.createTempDirectory("smartfile-image-folder-")
    try {
      writePng(dir.resolve("cat.png").toFile, width = 3, height = 2)
      writePng(dir.resolve("dog.png").toFile, width = 4, height = 5)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val result = desc.runInference()
      assert(result.format == SmartFileFormat.IMAGE)
      assert(result.isFolder)
      assert(result.fileCount == 2)
      assert(result.schema.getAttributeNames == List("image", "format", "width", "height"))
      assert(result.schema.getAttribute("image").getType == AttributeType.BINARY)
      assert(result.schema.getAttribute("format").getType == AttributeType.STRING)
      assert(result.schema.getAttribute("width").getType == AttributeType.INTEGER)
      assert(result.schema.getAttribute("height").getType == AttributeType.INTEGER)
    } finally deleteRecursively(dir)
  }

  it should "append a source file column when folder provenance is enabled" in {
    val dir = Files.createTempDirectory("smartfile-folder-source-column-")
    try {
      Files.writeString(dir.resolve("2025-01.csv"), "id,name\n1,Ada\n", StandardCharsets.UTF_8)
      Files.writeString(dir.resolve("2025-02.csv"), "id,name\n2,Lin\n", StandardCharsets.UTF_8)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))
      desc.includeSourceFile = true

      val schema = desc.sourceSchema()
      assert(schema.getAttributeNames == List("id", "name", "source_file"))
      assert(schema.getAttribute("source_file").getType == AttributeType.STRING)
    } finally deleteRecursively(dir)
  }

  it should "reject folders that mix file formats" in {
    val dir = Files.createTempDirectory("smartfile-mixed-folder-")
    try {
      Files.writeString(dir.resolve("part.csv"), "id,name\n1,Ada\n", StandardCharsets.UTF_8)
      Files.writeString(dir.resolve("part.jsonl"), """{"id":2,"name":"Lin"}""" + "\n", StandardCharsets.UTF_8)

      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val err = intercept[IllegalArgumentException](desc.runInference())
      assert(err.getMessage.contains("same detected format"))
    } finally deleteRecursively(dir)
  }

  it should "reject empty folders" in {
    val dir = Files.createTempDirectory("smartfile-empty-folder-")
    try {
      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(dir.toString)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val err = intercept[IllegalArgumentException](desc.runInference())
      assert(err.getMessage.contains("does not contain any readable files"))
    } finally deleteRecursively(dir)
  }

  it should "infer schema from a generated Excel file" in {
    val tmp = Files.createTempFile("smartfile-test-", ".xlsx").toFile
    try {
      writeExcel(tmp)
      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(tmp.getAbsolutePath)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val result = desc.runInference()
      assert(result.format == SmartFileFormat.EXCEL)
      val attrs = result.schema.getAttributes
      assert(attrs.length == 3)
      assert(attrs.head.getName == "id")
      assert(attrs(1).getName == "name")
      assert(attrs(2).getName == "score")
      assert(attrs.head.getType == AttributeType.INTEGER)
      assert(attrs(2).getType == AttributeType.DOUBLE)
    } finally tmp.delete()
  }

  it should "infer schema from a generated Parquet file" in {
    val tmp = Files.createTempFile("smartfile-test-", ".parquet").toFile
    tmp.delete() // ParquetWriter wants to create the file itself
    try {
      writeParquet(tmp)
      val desc = new SmartFileSourceOpDesc()
      desc.fileName = Some(tmp.getAbsolutePath)
      desc.setResolvedFileName(FileResolver.resolve(desc.fileName.get))

      val result = desc.runInference()
      assert(result.format == SmartFileFormat.PARQUET)
      val attrs = result.schema.getAttributes
      assert(attrs.length == 3)
      assert(attrs.exists(_.getName == "id"))
      assert(result.schema.getAttribute("id").getType == AttributeType.INTEGER)
      assert(result.schema.getAttribute("name").getType == AttributeType.STRING)
      assert(result.schema.getAttribute("score").getType == AttributeType.DOUBLE)
    } finally tmp.delete()
  }

  private def writeExcel(out: File): Unit = {
    val workbook = new XSSFWorkbook()
    try {
      val sheet = workbook.createSheet("Sheet1")
      val header = sheet.createRow(0)
      header.createCell(0).setCellValue("id")
      header.createCell(1).setCellValue("name")
      header.createCell(2).setCellValue("score")

      val rows = Seq((1, "Ada", 36.5), (2, "Lin", 29.1), (3, "Bob", 42.0))
      rows.zipWithIndex.foreach {
        case ((id, name, score), i) =>
          val row = sheet.createRow(i + 1)
          row.createCell(0).setCellValue(id.toDouble)
          row.createCell(1).setCellValue(name)
          row.createCell(2).setCellValue(score)
      }
      val fos = new FileOutputStream(out)
      try workbook.write(fos)
      finally fos.close()
    } finally workbook.close()
  }

  private def writePng(out: File, width: Int, height: Int): Unit = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    ImageIO.write(image, "png", out)
  }

  private def writeParquet(out: File): Unit = {
    val schemaStr =
      """
        |message simple {
        |  required int32 id;
        |  required binary name (UTF8);
        |  required double score;
        |}
      """.stripMargin
    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val conf = new Configuration(false)
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    GroupWriteSupport.setSchema(schema, conf)

    val factory = new SimpleGroupFactory(schema)
    val writer = new ParquetWriter[org.apache.parquet.example.data.Group](
      new Path(out.toURI),
      new GroupWriteSupport(),
      org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      true,
      false,
      ParquetWriter.DEFAULT_WRITER_VERSION,
      conf
    )
    try {
      writer.write(factory.newGroup().append("id", 1).append("name", "Ada").append("score", 36.5d))
      writer.write(factory.newGroup().append("id", 2).append("name", "Lin").append("score", 29.1d))
    } finally writer.close()

    // Avoid compiler unused-import warning for Type — keep an explicit reference here so that
    // if MessageTypeParser ever changes its return type the compile fails loudly.
    val _: Type = schema
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
}
