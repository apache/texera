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
import org.apache.texera.amber.core.state.NoLoopVariable
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
  @NoLoopVariable
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

    // Every file opens binary and is decoded by `_texera_decode`, which reads the
    // bytes the way the engine's reader does (see that helper). A whole file keeps
    // its text as decoded, so a CRLF stays a CRLF. Lines go through a StringIO in
    // universal-newline mode, which ends a line where `open(..., "r")` does, and so
    // where the engine does.
    val readWholeOf = if (isBinary) "_f.read()" else s"_texera_decode(_f.read(), $encLit)"
    val linesOf = s"io.StringIO(_texera_decode(_f.read(), $encLit), newline=None)"

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
        (" " * 16, "_name", readWholeOf, linesOf)
      } else {
        buf += "    with open(_fn, \"rb\") as _f:"
        (" " * 8, "_fn", readWholeOf, linesOf)
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
        case FileAttributeType.INTEGER   => "_texera_parse_int(l, 32)"
        case FileAttributeType.LONG      => "_texera_parse_int(l, 64)"
        case FileAttributeType.DOUBLE    => "_texera_java_double(l)"
        case FileAttributeType.BOOLEAN   => TextSourceOpDesc.BooleanParserCall
        case FileAttributeType.TIMESTAMP => "l.rstrip()"
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
    buf ++= TextSourceOpDesc.parseTimestampColumn(attributeType, colLit)

    buf.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] = {
    val isBinary =
      attributeType == FileAttributeType.BINARY || attributeType == FileAttributeType.LARGE_BINARY
    TextSourceOpDesc.lineHelpers(attributeType) ++
      (if (isBinary) Seq.empty else Seq(FileScanOpDesc.DecodeHelper))
  }

  override def standaloneImports(): Seq[String] = {
    val readsLines =
      !attributeType.isSingle && attributeType != FileAttributeType.BINARY &&
        attributeType != FileAttributeType.LARGE_BINARY
    (if (readsLines) Seq("import io") else Seq.empty) ++
      (if (extract) Seq("import zipfile") else Seq.empty)
  }
}

object FileScanOpDesc {

  /** The engine decodes through Java's InputStreamReader, which puts U+FFFD in
    * place of any bytes it cannot decode rather than failing, and reads UTF-16
    * with no byte-order mark as big-endian. Python's codecs refuse both, so a file
    * read under the wrong encoding ended the script where the engine read on.
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
