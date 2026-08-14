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

package org.apache.texera.amber.operator.source.scan.arrow

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.util.Using

@JsonIgnoreProperties(value = Array("fileEncoding"))
class ArrowSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  fileTypeName = Option("Arrow")

  override def generateStandaloneCode(): String = {
    val rawPath = fileName.getOrElse("")
    val basename = Paths.get(new URI(rawPath).getPath).getFileName.toString
    val read = s"""out1df = pd.read_feather(${pyStringLiteral(basename)})"""
    // A timestamp column needs nothing here. The file names UTC and holds the
    // wall clock as UTC, so pd.read_feather and the executor read the same
    // reading off it — no zone of the reader's own enters either side. The other
    // scan sources have to name their date columns, CSV and JSONL carrying no
    // types to go on, but Arrow states its own.
    //
    // The executor drops `offset` rows and then takes `limit` of them. Feather has
    // no row-range read, so the same window is taken once the frame is in memory.
    val window = (offset, limit) match {
      case (Some(o), Some(l)) => Some(s"$o:${o + l}")
      case (Some(o), None)    => Some(s"$o:")
      case (None, Some(l))    => Some(s":$l")
      case _                  => None
    }
    (read +: window.map(w => s"out1df = out1df.iloc[$w].reset_index(drop=True)").toSeq)
      .mkString("\n")
  }

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.scan.arrow.ArrowSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> inferSchema()))
      )
  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    *
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    require(
      fileResolved(),
      "No file selected. Please select a valid .arrow file from the 'File' dropdown in the right panel."
    )

    val uri = new URI(fileName.get)
    if (uri.getScheme == "file") {
      require(
        new java.io.File(uri).isFile,
        "The selected item is a folder or does not exist. Please select an actual .arrow file from the 'File' dropdown."
      )
    }
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()

    val allocator = new RootAllocator()

    Using
      .Manager { use =>
        val channel = use(Files.newByteChannel(file.toPath, StandardOpenOption.READ))
        val reader = use(new ArrowFileReader(channel, allocator))
        val arrowSchema: ArrowSchema = reader.getVectorSchemaRoot.getSchema
        ArrowUtils.toTexeraSchema(arrowSchema)
      }
      .recoverWith {
        case scala.util.control.NonFatal(e) =>
          scala.util.Failure(
            new RuntimeException(
              "Failed to read the .arrow file. Please ensure it is a valid Arrow file.",
              e
            )
          )
      }
      .get
  }
}
