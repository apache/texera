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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, StandardOpenOption}
import scala.util.{Try, Using}

@JsonIgnoreProperties(value = Array("fileEncoding"))
class ArrowSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  fileTypeName = Option("Arrow")

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    val read = s"""out1df = pd.read_feather($SourceFilePlaceholder)"""
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

    // A LONG column holding a null comes back through a float, which rounds every
    // value past 2^53: the file's 9007199254740993 reads as ...992, where the
    // executor hands the exact value on. Re-reading just those columns as the
    // nullable integer leaves every other column's type as it was.
    // inferSchema, not sourceSchema: this operator reads its types out of the
    // file and leaves the base's sourceSchema returning null. A file that cannot
    // be read leaves the re-read off rather than failing the export.
    val longColumns: Seq[String] =
      Try(inferSchema()).toOption.toSeq.flatMap(
        _.getAttributes
          .filter(_.getType == AttributeType.LONG)
          .map(a => pyStringLiteral(a.getName))
      )
    val exactLongs = longColumns.map { name =>
      s"out1df[$name] = pd.read_feather($SourceFilePlaceholder, columns=[$name], " +
        s"""dtype_backend="numpy_nullable")[$name]"""
    }

    // The re-read is of the whole file, so it happens before the window is taken.
    ((read +: exactLongs) ++ window.map(w => s"out1df = out1df.iloc[$w].reset_index(drop=True)"))
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
