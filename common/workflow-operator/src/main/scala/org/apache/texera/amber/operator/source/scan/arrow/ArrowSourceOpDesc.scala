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
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, StandardOpenOption}
import scala.util.Using

@JsonIgnoreProperties(value = Array("fileEncoding"))
class ArrowSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  fileTypeName = Option("Arrow")

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    // Arrow says of every value whether it is there, and a numpy column has
    // nowhere to put that: a missing double and a stored NaN both land on NaN.
    // The nullable dtypes keep a holed integer integral too, so a long past 2^53
    // is not rounded on its way through a float.
    val read =
      s"""out1df = pd.read_feather($SourceFilePlaceholder, dtype_backend="numpy_nullable")"""
    // The widths pandas keeps and Texera has no column for. A file states the
    // width and the sign of each of its numbers, and pandas reads every one of
    // them back, where a Texera column is a double or a 32-bit integer and
    // nothing narrower. Left alone, a single-precision column summed to a
    // different number on the two sides: 16777216 and 1 add to 16777217 as
    // doubles and to 16777216 as floats. An unsigned column lands on the Texera
    // type its own values need, which is the executor's rule too: one counting
    // to 4294967295 has to be a long. See ParquetScanSourceOpDesc, which
    // normalizes the same widths for the same reason.
    val widths =
      """|for _column, _values in out1df.items():
         |    if _values.dtype == "Float32":
         |        out1df[_column] = _values.astype("Float64")
         |    elif _values.dtype in ("Int8", "Int16", "UInt8", "UInt16"):
         |        out1df[_column] = _values.astype("Int32")
         |    elif _values.dtype == "UInt32":
         |        out1df[_column] = _values.astype("Int64")""".stripMargin
    // A timestamp column needs nothing here. The file names UTC and holds the
    // wall clock as UTC, so pd.read_feather and the executor read the same
    // reading off it — no zone of the reader's own enters either side. The other
    // scan sources have to name their date columns, CSV and JSONL carrying no
    // types to go on, but Arrow states its own.
    //
    // The executor drops `offset` rows and then takes `limit` of them. Feather has
    // no row-range read, so the same window is taken once the frame is in memory.
    //
    // Clamped first: the property editor refuses a negative, but a plan posted to
    // the API can still carry one, and `iloc` reads it from the end where `drop`
    // skips nothing and `take` keeps nothing.
    val window = (offset.map(_.max(0)), limit.map(_.max(0))) match {
      // The end of the window is counted in Long: two Ints the operator accepts
      // can add up past what an Int holds, and the slice would come out negative
      // and take the wrong rows. As in ParquetScanSourceOpDesc.
      case (Some(o), Some(l)) => Some(s"$o:${o.toLong + l}")
      case (Some(o), None)    => Some(s"$o:")
      case (None, Some(l))    => Some(s":$l")
      case _                  => None
    }

    (Seq(read, widths) ++ window.map(w => s"out1df = out1df.iloc[$w].reset_index(drop=True)"))
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
