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

package org.apache.texera.amber.operator.source.scan.file

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.state.NoLoopVariable
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.operator.source.scan.text.TextSourceOpDesc
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

class FileScanOpDesc
    extends SourceOperatorDescriptor
    with TextSourceOpDesc
    with StandaloneCodeGenerator {
  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("Encoding")
  var fileEncoding: FileDecodingMethod = FileDecodingMethod.UTF_8

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Extract")
  val extract: Boolean = false

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Include Filename")
  @NoLoopVariable
  var outputFileName: Boolean = false

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    TextSourceOpDesc.buildPhysicalOp(
      this,
      workflowId,
      executionId,
      "org.apache.texera.amber.operator.source.scan.file.FileScanOpExec"
    )

  override def sourceSchema(): Schema =
    TextSourceOpDesc.buildFileScanSchema(attributeName, attributeType, outputFileName)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "File Scan From Input",
      operatorDescription = "Scan data from file paths provided by input tuples",
      operatorGroupName = OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List(InputPort(displayName = "Filename")),
      outputPorts = List(OutputPort())
    )

  override def generateStandaloneCode(): String = {
    val enc = fileEncoding.toString.replace("_", "-").toLowerCase
    val encLit = pyStringLiteral(enc)
    val colLit = pyStringLiteral(attributeName)
    val buf = scala.collection.mutable.ArrayBuffer[String]()

    // The executor takes the row's first String field, not its first column.
    buf += "def _texera_file_name(row):"
    buf += "    for _v in row:"
    buf += "        if isinstance(_v, str):"
    buf += "            return _v"
    buf += """    raise ValueError(f"no file name in row: {row!r}")"""
    buf += ""
    buf += "_rows = []"
    buf += "for _fn in (_texera_file_name(r) for r in in1df.itertuples(index=False)):"

    val readWholeOf =
      if (TextSourceOpDesc.isBinaryAttribute(attributeType)) "_f.read()"
      else s"_texera_decode(_f.read(), $encLit)"
    val linesOf = s"io.StringIO(_texera_decode(_f.read(), $encLit), newline=None)"

    val (indent, nameExpr) =
      if (extract) {
        buf ++= TextSourceOpDesc.buildZipExtractLines("_fn").map("    " + _)
        (" " * 16, "_name")
      } else {
        buf += """    with open(_fn, "rb") as _f:"""
        (" " * 8, "_fn")
      }

    TextSourceOpDesc.appendFileScanRows(
      buf,
      indent,
      nameExpr,
      readWholeOf,
      linesOf,
      attributeType,
      outputFileName,
      fileScanOffset,
      fileScanLimit
    )
    TextSourceOpDesc.appendFileScanDataFrame(buf, attributeType, colLit, outputFileName)
    buf.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    TextSourceOpDesc.fileScanStandaloneHelpers(attributeType, FileScanOpDesc.DecodeHelper)

  override def standaloneImports(): Seq[String] =
    TextSourceOpDesc.fileScanStandaloneImports(attributeType, extract)
}

object FileScanOpDesc {

  /** The engine decodes through Java's InputStreamReader, which puts U+FFFD in
    * place of any bytes it cannot decode rather than failing, and reads UTF-16
    * with no byte-order mark as big-endian.
    */
  val DecodeHelper: String =
    """def _texera_decode(raw, encoding):
      |    if encoding == "utf-16":
      |        if raw[:2] == b"\xfe\xff":
      |            return raw[2:].decode("utf-16-be", "replace")
      |        if raw[:2] == b"\xff\xfe":
      |            return raw[2:].decode("utf-16-le", "replace")
      |        return raw.decode("utf-16-be", "replace")
      |    return raw.decode(encoding, "replace")""".stripMargin
}
