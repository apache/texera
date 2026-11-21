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

package org.apache.texera.amber.operator.source.scan.csv

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import java.net.URI
import scala.util.Try

/** Shared URI/delimiter resolution, schema building, and standalone codegen for CSV scans. */
private[source] object CsvScanSupport {

  def resolveDelimiterChar(customDelimiter: Option[String]): String =
    customDelimiter.filter(_.nonEmpty).getOrElse(",").charAt(0).toString

  def resolveCsvUriAndDelimiter(
      desc: ScanSourceOpDesc,
      customDelimiter: Option[String]
  ): (URI, Char) = {
    val delimiterChar = resolveDelimiterChar(customDelimiter).charAt(0)
    require(
      desc.fileResolved(),
      "No file selected. Please select a valid .csv file from the 'File' dropdown in the right panel."
    )
    val uri = new URI(desc.fileName.get)
    if (uri.getScheme == "file") {
      require(
        new java.io.File(uri).isFile,
        "The selected item is a folder or does not exist. Please select an actual .csv file from the 'File' dropdown."
      )
    }
    (uri, delimiterChar)
  }

  def buildSchemaFromFirstRow(
      firstRow: Array[String],
      attributeTypeList: Array[AttributeType],
      hasHeader: Boolean
  ): Schema =
    Schema().add(firstRow.indices.map { i =>
      new Attribute(
        if (hasHeader && firstRow(i).nonEmpty) firstRow(i) else s"column-${i + 1}",
        attributeTypeList.lift(i).getOrElse(AttributeType.STRING)
      )
    })

  def generateStandaloneCode(
      desc: ScanSourceOpDesc,
      customDelimiter: Option[String],
      hasHeader: Boolean,
      dtypeByType: Map[AttributeType, String],
      includeNaValuesBlank: Boolean,
      includeWindow: Boolean,
      declaredSchemaBody: (String, Schema) => String,
      ignoredWindowPrefix: Seq[String] = Seq.empty
  ): String = {
    val sep = resolveDelimiterChar(customDelimiter)
    val schema = Try(desc.sourceSchema()).toOption
    val encoding = desc.fileEncoding.toString.replace("_", "-").toLowerCase
    val headerArg = if (hasHeader) "0" else "None"

    val args = scala.collection.mutable.ArrayBuffer[String]()
    args += s"filepath_or_buffer=$SourceFilePlaceholder"
    args += s"sep=${pyStringLiteral(sep)}"
    args += s"""encoding=${pyStringLiteral(encoding)}"""
    args += s"header=$headerArg"
    args += "keep_default_na=False"
    if (includeNaValuesBlank) args += """na_values=[""]"""

    val typedColumns: Seq[String] =
      schema.toSeq.flatMap(
        _.getAttributes.zipWithIndex.flatMap {
          case (attribute, i) => dtypeByType.get(attribute.getType).map(d => s"""$i: "$d"""")
        }
      )
    if (typedColumns.nonEmpty) args += s"dtype={${typedColumns.mkString(", ")}}"

    if (includeWindow) {
      desc.offset.map(_.max(0)).foreach { o =>
        if (hasHeader) args += s"skiprows=lambda _i: 0 < _i <= ${o.toLong}"
        else args += s"skiprows=$o"
      }
      desc.limit.map(_.max(0)).foreach(l => args += s"nrows=$l")
      if (!hasHeader)
        schema.map(_.getAttributes.size).filter(_ > 0).foreach { n =>
          args += s"names=[${(0 until n).mkString(", ")}]"
        }
    }

    val readCall = s"out1df = pd.read_csv(${args.mkString(", ")})"

    val body = schema.filter(_.getAttributes.nonEmpty) match {
      case Some(declared)    => declaredSchemaBody(readCall, declared)
      case None if hasHeader => readCall
      case None =>
        s"""$readCall
           |out1df.columns = [f"column-{i + 1}" for i in range(len(out1df.columns))]""".stripMargin
    }

    (ignoredWindowPrefix :+ body).mkString("\n")
  }
}
