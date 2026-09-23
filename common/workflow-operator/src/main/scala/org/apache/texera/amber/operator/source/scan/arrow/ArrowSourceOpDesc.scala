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

  // pyarrow is what reads an Arrow file, pandas going through it; the block
  // below asks it for the file's own columns.
  override def standaloneImports(): Seq[String] = Seq("import pyarrow as pa")

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    // Arrow says of every value whether it is there, and a numpy column has
    // nowhere to put that: a missing double and a stored NaN both land on NaN.
    // The nullable dtypes keep a holed integer integral too, so a long past 2^53
    // is not rounded on its way through a float. Named here because the read
    // below cannot ask pd.read_feather for them, and named for the types
    // ArrowUtils.toAttributeType reads; a timestamp needs none, datetime64
    // having a slot of its own for a missing value.
    val dtypes =
      """|_nullable = {
         |    pa.bool_(): pd.BooleanDtype(),
         |    pa.string(): pd.StringDtype(),
         |    pa.float32(): pd.Float32Dtype(),
         |    pa.float64(): pd.Float64Dtype(),
         |    pa.int8(): pd.Int8Dtype(),
         |    pa.int16(): pd.Int16Dtype(),
         |    pa.int32(): pd.Int32Dtype(),
         |    pa.int64(): pd.Int64Dtype(),
         |    pa.uint8(): pd.UInt8Dtype(),
         |    pa.uint16(): pd.UInt16Dtype(),
         |    pa.uint32(): pd.UInt32Dtype(),
         |}""".stripMargin
    // A file pandas wrote notes in its schema what the frame it came from looked
    // like, and pandas reads that note back: a column the frame was keyed by
    // returns as the index, and numbered labels return as numbers where the file
    // says "1" and "2". The executor reads the columns the file states, so the
    // note is refused rather than undone, as the Parquet source refuses it.
    val read =
      s"""|with pa.ipc.open_file($SourceFilePlaceholder) as _file:
          |    _table = _file.read_all()
          |out1df = _table.to_pandas(ignore_metadata=True, types_mapper=_nullable.get)""".stripMargin
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
    // A timestamp column may name a zone, and pandas keeps it on the column
    // where the executor keeps only the wall clock in that zone: a Texera
    // TIMESTAMP has none. Left zoned, the column reached a downstream `astype`
    // that refuses to drop a zone and ended the script. pandas already holds the
    // wall clock in the file's own zone, so the zone is taken off and the clock
    // left as it is, which is what ArrowUtils reads. The other scan sources have
    // to name their date columns, CSV and JSONL carrying no types to go on, but
    // Arrow states its own.
    val zones =
      """|for _column, _values in out1df.items():
         |    if isinstance(_values.dtype, pd.DatetimeTZDtype):
         |        out1df[_column] = _values.dt.tz_localize(None)""".stripMargin

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

    (Seq(dtypes, read, widths, zones) ++ window.map(w =>
      s"out1df = out1df.iloc[$w].reset_index(drop=True)"
    )).mkString("\n")
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
