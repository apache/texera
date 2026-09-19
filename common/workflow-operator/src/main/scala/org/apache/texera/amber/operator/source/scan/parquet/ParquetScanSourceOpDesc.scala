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

package org.apache.texera.amber.operator.source.scan.parquet

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI
import scala.util.Using

/**
  * Reads a Parquet file. The format states its own column types in a footer, so
  * unlike the CSV and JSONL sources this one infers nothing: the INTEGER the
  * file was written with is the INTEGER that arrives.
  */
@JsonIgnoreProperties(value = Array("fileEncoding"))
class ParquetScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  fileTypeName = Option("Parquet")

  // A DECIMAL is the one column pandas does not land on the same type as the
  // executor: it fills that column with decimal.Decimal objects, which a script
  // cannot then multiply by a float.
  override def standaloneImports(): Seq[String] = Seq("from decimal import Decimal")

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    // No date columns to name, and no dtype map. pandas reads the types out of
    // the same footer the executor does, which is the whole point of the format;
    // the text formats have to be told because they carry nothing to read.
    // Read into the nullable dtypes, as the Arrow source is. Parquet says of
    // every value whether it is there, and a numpy column has nowhere to put
    // that: pandas widens a holed integer column through a float, where every
    // value past 2^53 is rounded and 9007199254740993 came back as ...992. The
    // executor reads the exact long, and the declared column stays integral.
    val read =
      s"""out1df = pd.read_parquet($SourceFilePlaceholder, dtype_backend="numpy_nullable")"""
    // The columns pandas does not land on the same value as the executor. A
    // DECIMAL arrives as decimal.Decimal objects and a FLOAT keeps the single
    // precision the executor widens; an unsigned column, a duration and a zoned
    // timestamp come back as the Arrow types the writer left in the file's
    // metadata, where the executor reads what the footer alone states.
    //
    // Named in the nullable spelling, `Float32` and not `float32`, and widened
    // into the nullable dtype in turn, so a hole stays a hole rather than
    // becoming the NaN a numpy column would have to write it as.
    val columns =
      """|for _column, _values in out1df.items():
         |    if isinstance(next(iter(_values.dropna()), None), Decimal):
         |        out1df[_column] = _values.astype("Float64")
         |    elif isinstance(_values.dtype, pd.DatetimeTZDtype):
         |        out1df[_column] = _values.dt.tz_convert("UTC").dt.tz_localize(None)
         |    elif _values.dtype.kind in "um":
         |        out1df[_column] = _values.astype("Int64")
         |    elif _values.dtype == "Float32":
         |        out1df[_column] = _values.astype("Float64")""".stripMargin
    // The executor drops `offset` rows and then takes `limit` of them. Parquet
    // can skip whole row groups but not an arbitrary row range, so the same
    // window is taken once the frame is in memory, as the Arrow source does.
    val window = (offset, limit) match {
      // The end of the window is counted in Long: two Ints the operator accepts
      // can add up past what an Int holds, and the slice would come out negative
      // and take the wrong rows.
      case (Some(o), Some(l)) => Some(s"$o:${o.toLong + l}")
      case (Some(o), None)    => Some(s"$o:")
      case (None, Some(l))    => Some(s":$l")
      case _                  => None
    }
    (Seq(read, columns) ++ window.map(w => s"out1df = out1df.iloc[$w].reset_index(drop=True)"))
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
          "org.apache.texera.amber.operator.source.scan.parquet.ParquetScanSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> inferSchema()))
      )
  }

  /** The file's own schema, read from its footer. No rows are read to get it. */
  @Override
  def inferSchema(): Schema = {
    require(
      fileResolved(),
      "No file selected. Please select a valid .parquet file from the 'File' dropdown in the right panel."
    )

    val uri = new URI(fileName.get)
    if (uri.getScheme == "file") {
      require(
        new java.io.File(uri).isFile,
        "The selected item is a folder or does not exist. Please select an actual .parquet file from the 'File' dropdown."
      )
    }
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()

    Using(ParquetFileReader.open(new LocalInputFile(file.toPath))) { reader =>
      ParquetSchemaMapping.toTexeraSchema(reader.getFooter.getFileMetaData.getSchema)
    }.recoverWith {
      case e: UnsupportedOperationException => scala.util.Failure(e)
      case scala.util.control.NonFatal(e) =>
        scala.util.Failure(
          new RuntimeException(
            "Failed to read the .parquet file. Please ensure it is a valid Parquet file.",
            e
          )
        )
    }.get
  }
}
