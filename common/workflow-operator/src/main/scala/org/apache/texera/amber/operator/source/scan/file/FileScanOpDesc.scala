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
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, FileDecodingMethod}
import org.apache.texera.amber.operator.source.scan.text.TextSourceOpDesc
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

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
  var outputFileName: Boolean = false

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
          "org.apache.texera.amber.operator.source.scan.file.FileScanOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def sourceSchema(): Schema = {
    var schema = Schema()
    if (outputFileName) {
      schema = schema.add("filename", AttributeType.STRING)
    }
    schema.add(attributeName, attributeType.getType)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "File Scan From Input",
      operatorDescription = "Scan data from file paths provided by input tuples",
      operatorGroupName = OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List(InputPort(displayName = "Filename")),
      outputPorts = List(OutputPort())
    )

  override def generateStandaloneCode(): String = {
    val col = attributeName
    val enc = fileEncoding.toString.replace("_", "-").toLowerCase
    val buf = scala.collection.mutable.ArrayBuffer[String]()

    if (extract)
      buf += "# WARNING: extract=true is not supported in standalone mode; files are read as-is, not unpacked from archives."

    val isBinary =
      attributeType == FileAttributeType.BINARY || attributeType == FileAttributeType.LARGE_BINARY
    val openArgs =
      if (isBinary) """"rb""""
      else s""""r", encoding=${pyStringLiteral(enc)}"""

    // The executor takes the row's first String field, not its first column, so a row that
    // carries an id ahead of the path still finds the path. Reading column 0 opened the id.
    // A row with no string at all makes the executor's `.get` throw, so this raises too
    // rather than quietly skipping the row.
    buf += "def _texera_file_name(row):"
    buf += "    for _v in row:"
    buf += "        if isinstance(_v, str):"
    buf += "            return _v"
    buf += """    raise ValueError(f"no file name in row: {row!r}")"""
    buf += ""
    buf += "_rows = []"
    buf += "for _fn in (_texera_file_name(r) for r in in1df.itertuples(index=False)):"
    buf += s"    with open(_fn, $openArgs) as _f:"

    // Match the platform (FileScanUtils.createTuplesFromFile): its line-by-line
    // branch ignores outputFileName and emits only the value, so the filename
    // column is added ONLY in single-value mode.
    val emitFilename = outputFileName && attributeType.isSingle

    if (attributeType.isSingle) {
      if (emitFilename) buf += "        _rows.append((_fn, _f.read()))"
      else buf += "        _rows.append(_f.read())"
    } else {
      val castExpr = attributeType match {
        case FileAttributeType.INTEGER   => "int(l.rstrip())"
        case FileAttributeType.LONG      => "int(l.rstrip())"
        case FileAttributeType.DOUBLE    => "float(l.rstrip())"
        case FileAttributeType.BOOLEAN   => """l.rstrip().lower() == "true""""
        case FileAttributeType.TIMESTAMP => "pd.Timestamp(l.rstrip())"
        case _                           => """l.rstrip("\n")"""
      }
      // The slice applies to the raw lines, as the engine drops and takes
      // before parsing: a line outside the window is never converted, so an
      // unparseable one there costs nothing. Taking after dropping also keeps
      // a large limit from overflowing the end index.
      val linesExpr =
        if (fileScanOffset.isEmpty && fileScanLimit.isEmpty) "_f"
        else {
          val dropped =
            fileScanOffset.filter(_ > 0).fold("_f.readlines()")(o => s"_f.readlines()[$o:]")
          fileScanLimit.fold(dropped)(l => s"$dropped[:${l.max(0)}]")
        }
      buf += s"        _rows.extend($castExpr for l in $linesExpr)"
    }

    val colLit = pyStringLiteral(col)
    if (emitFilename) {
      buf += s"""out1df = pd.DataFrame(_rows, columns=["filename", $colLit])"""
    } else {
      buf += s"""out1df = pd.DataFrame({$colLit: _rows})"""
    }

    buf.mkString("\n")
  }
}
