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

    val isBinary =
      attributeType == FileAttributeType.BINARY || attributeType == FileAttributeType.LARGE_BINARY
    val encLit = pyStringLiteral(enc)

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

    // With extract on the engine reads the files INSIDE the archive, one tuple
    // per entry, and skips the entries macOS adds. `zipfile` is the reader that
    // matches: the platform casts what it opened to a ZipArchiveInputStream, so
    // an archive that is not a zip fails on both sides. A directory entry is
    // read, not skipped, there and here alike — it yields nothing.
    val (indent, nameExpr, readWhole, lines) =
      if (extract) {
        buf += "    with zipfile.ZipFile(_fn) as _z:"
        buf += "        for _name in _z.namelist():"
        buf += """            if _name.startswith("__MACOSX"):"""
        buf += "                continue"
        buf += "            with _z.open(_name) as _f:"
        // A zip entry only opens binary, so text is decoded here rather than by
        // the reader. TextIOWrapper, not splitlines: it ends a line where
        // `open(..., "r")` does, which is what the non-extract branch reads with.
        (
          " " * 16,
          "_name",
          if (isBinary) "_f.read()" else s"_f.read().decode($encLit)",
          s"io.TextIOWrapper(_f, encoding=$encLit)"
        )
      } else {
        val openArgs = if (isBinary) """"rb"""" else s""""r", encoding=$encLit"""
        buf += s"    with open(_fn, $openArgs) as _f:"
        (" " * 8, "_fn", "_f.read()", "_f")
      }

    // Whatever the flag says, as the platform now reads it: every row carries the
    // name of the file its value came from, a line's as much as a whole file's.
    // See FileScanUtils.createTuplesFromFile.
    val emitFilename = outputFileName

    if (attributeType.isSingle) {
      if (emitFilename) buf += s"${indent}_rows.append(($nameExpr, $readWhole))"
      else buf += s"${indent}_rows.append($readWhole)"
    } else {
      val castExpr = attributeType match {
        case FileAttributeType.INTEGER   => "int(l.rstrip())"
        case FileAttributeType.LONG      => "int(l.rstrip())"
        case FileAttributeType.DOUBLE    => "float(l.rstrip())"
        case FileAttributeType.BOOLEAN   => TextSourceOpDesc.BooleanParserCall
        case FileAttributeType.TIMESTAMP => "pd.Timestamp(l.rstrip())"
        case _                           => """l.rstrip("\n")"""
      }
      // The slice applies to the raw lines, as the engine drops and takes
      // before parsing: a line outside the window is never converted, so an
      // unparseable one there costs nothing. Taking after dropping also keeps
      // a large limit from overflowing the end index. The window is per entry,
      // as the engine's is: it drops and takes inside the flatMap over entries.
      val linesExpr =
        if (fileScanOffset.isEmpty && fileScanLimit.isEmpty) lines
        else {
          val dropped =
            fileScanOffset
              .filter(_ > 0)
              .fold(s"$lines.readlines()")(o => s"$lines.readlines()[$o:]")
          fileScanLimit.fold(dropped)(l => s"$dropped[:${l.max(0)}]")
        }
      val row = if (emitFilename) s"($nameExpr, $castExpr)" else castExpr
      buf += s"${indent}_rows.extend($row for l in $linesExpr)"
    }

    val colLit = pyStringLiteral(col)
    if (emitFilename) {
      buf += s"""out1df = pd.DataFrame(_rows, columns=["filename", $colLit])"""
    } else {
      buf += s"""out1df = pd.DataFrame({$colLit: _rows})"""
    }

    buf.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    if (attributeType == FileAttributeType.BOOLEAN) Seq(TextSourceOpDesc.BooleanParser)
    else Seq.empty

  override def standaloneImports(): Seq[String] =
    if (!extract) Seq.empty
    else if (attributeType.isSingle) Seq("import zipfile")
    else Seq("import io", "import zipfile")
}
