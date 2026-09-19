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

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.source.scan.text.TextSourceOpDesc
import org.apache.texera.amber.operator.source.scan.{
  FileAttributeType,
  FileDecodingMethod,
  ScanSourceOpDesc
}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

@JsonIgnoreProperties(value = Array("limit", "offset", "fileEncoding"))
class FileScanSourceOpDesc
    extends ScanSourceOpDesc
    with TextSourceOpDesc
    with StandaloneCodeGenerator {
  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("Encoding")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "binary")
    )
  )
  // The charset the panel offers, and the one the executor decodes with. The
  // inherited `fileEncoding` is named in this class's @JsonIgnoreProperties, so
  // it never survives the trip into the executor: reading that one there left
  // every file decoded as UTF-8 whatever was chosen. The name is kept as the
  // panel spells it, a saved workflow carrying `encoding` and not the other.
  val encoding: FileDecodingMethod = FileDecodingMethod.UTF_8

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Extract")
  val extract: Boolean = false

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Include Filename")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "extract"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  val outputFileName: Boolean = false

  fileTypeName = Option("")

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    val col = attributeName
    // `encoding` is the charset the panel offers, and the one the executor now
    // decodes with.
    val enc = encoding.toString.replace("_", "-").toLowerCase
    val colLit = pyStringLiteral(col)
    val encLit = pyStringLiteral(enc)
    val buf = scala.collection.mutable.ArrayBuffer[String]()

    val isBinary =
      attributeType == FileAttributeType.BINARY || attributeType == FileAttributeType.LARGE_BINARY

    val castExpr = attributeType match {
      case FileAttributeType.INTEGER   => "int(l.rstrip())"
      case FileAttributeType.LONG      => "int(l.rstrip())"
      case FileAttributeType.DOUBLE    => "float(l.rstrip())"
      case FileAttributeType.BOOLEAN   => TextSourceOpDesc.BooleanParserCall
      case FileAttributeType.TIMESTAMP => "pd.Timestamp(l.rstrip())"
      case _                           => """l.rstrip("\n")"""
    }

    // The slice applies to the raw lines, as the engine drops and takes before
    // parsing: a line outside the window is never converted, so an unparseable
    // one there costs nothing. Taking after dropping also keeps a large limit
    // from overflowing the end index. With extract on, the window is per entry,
    // as the engine's is: it drops and takes inside the flatMap over entries.
    def windowed(lines: String): String =
      if (fileScanOffset.isEmpty && fileScanLimit.isEmpty) lines
      else {
        val dropped =
          fileScanOffset.filter(_ > 0).fold(s"$lines.readlines()")(o => s"$lines.readlines()[$o:]")
        fileScanLimit.fold(dropped)(l => s"$dropped[:${l.max(0)}]")
      }

    // Whatever the flag says, as the platform now reads it: every row carries
    // the name of the file its value came from, a line's as much as a whole
    // file's. See FileScanUtils.createTuplesFromFile.
    val emitFilename = outputFileName

    if (extract) {
      // The engine reads the files INSIDE the archive, one tuple per entry, and
      // skips the entries macOS adds. `zipfile` is the reader that matches: the
      // platform casts what it opened to a ZipArchiveInputStream, so an archive
      // that is not a zip fails on both sides. A directory entry is read, not
      // skipped, there and here alike — it yields nothing. The filename column
      // carries the ENTRY's name, which is what the engine puts there.
      buf += "_rows = []"
      buf += s"with zipfile.ZipFile($SourceFilePlaceholder) as _z:"
      buf += "    for _name in _z.namelist():"
      buf += """        if _name.startswith("__MACOSX"):"""
      buf += "            continue"
      buf += "        with _z.open(_name) as _f:"
      if (attributeType.isSingle) {
        // A zip entry only opens binary, so text is decoded here rather than by
        // the reader.
        val readWhole = if (isBinary) "_f.read()" else s"_f.read().decode($encLit)"
        if (emitFilename) buf += s"            _rows.append((_name, $readWhole))"
        else buf += s"            _rows.append($readWhole)"
      } else {
        // TextIOWrapper, not splitlines: it ends a line where `open(..., "r")`
        // does, which is what the branch below reads with.
        val linesExpr = windowed(s"io.TextIOWrapper(_f, encoding=$encLit)")
        val row = if (emitFilename) s"(_name, $castExpr)" else castExpr
        buf += s"            _rows.extend($row for l in $linesExpr)"
      }
      if (emitFilename) buf += s"""out1df = pd.DataFrame(_rows, columns=["filename", $colLit])"""
      else buf += s"""out1df = pd.DataFrame({$colLit: _rows})"""
    } else if (attributeType.isSingle) {
      val openArgs =
        if (isBinary) s"""$SourceFilePlaceholder, "rb""""
        else s"""$SourceFilePlaceholder, "r", encoding=$encLit"""
      val dfCols =
        if (emitFilename) s"""{"filename": $SourceFilePlaceholder, $colLit: [_f.read()]}"""
        else s"""{$colLit: [_f.read()]}"""
      buf += s"""with open($openArgs) as _f:"""
      buf += s"""    out1df = pd.DataFrame($dfCols)"""
    } else {
      val linesExpr = windowed("_f")
      val dfCols =
        if (emitFilename)
          s"""{"filename": $SourceFilePlaceholder, $colLit: [$castExpr for l in $linesExpr]}"""
        else s"""{$colLit: [$castExpr for l in $linesExpr]}"""
      buf += s"""with open($SourceFilePlaceholder, "r", encoding=$encLit) as _f:"""
      buf += s"""    out1df = pd.DataFrame($dfCols)"""
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
          "org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpExec",
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
}
