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
      else s""""r", encoding="$enc""""

    buf += "_rows = []"
    buf += "for _fn in in1df.iloc[:, 0]:"
    buf += s"    with open(_fn, $openArgs) as _f:"

    if (attributeType.isSingle) {
      if (outputFileName) buf += "        _rows.append((_fn, _f.read()))"
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
      val hasSlice = fileScanOffset.isDefined || fileScanLimit.isDefined
      if (hasSlice) {
        val start = fileScanOffset.getOrElse(0)
        val sliceExpr = fileScanLimit match {
          case Some(l) => s"_lines[$start:${start + l}]"
          case None    => s"_lines[$start:]"
        }
        buf += s"        _lines = [$castExpr for l in _f]"
        if (outputFileName) buf += s"    _rows.extend((_fn, _v) for _v in $sliceExpr)"
        else buf += s"    _rows.extend($sliceExpr)"
      } else {
        if (outputFileName) {
          buf += "        for l in _f:"
          buf += s"            _rows.append((_fn, $castExpr))"
        } else {
          buf += s"        _rows.extend($castExpr for l in _f)"
        }
      }
    }

    if (outputFileName) {
      buf += s"""out1df = pd.DataFrame(_rows, columns=["filename", "$col"])"""
    } else {
      buf += s"""out1df = pd.DataFrame({"$col": _rows})"""
    }

    buf.mkString("\n")
  }
}
