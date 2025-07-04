/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.amber.operator.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import play.api.libs.json.{JsObject, JsString, Json}

import java.io.InputStreamReader
import java.net.URI

class IntelligentCSVReaderOpDesc extends ScanSourceOpDesc {
  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var customDelimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  @transient private var cachedSchema: Option[Schema] = None

  fileTypeName = Option("CSV")

  private def schemaToJson(s: Schema): String = {
    val fields = s.getAttributeNames.zip(s.getAttributes).map {
      case (name, attr) => name -> JsString(attr.getType.name())
    }
    Json.stringify(JsObject(fields))
  }

  // ---------- main physical-plan construction ----------
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {

    // ▶︎ Generate the Python script on the fly
    val code: String = generatePythonCode(
      originalFileName, // full Texera dataset path
      customDelimiter, // optional delimiter
      hasHeader // header flag
    )

    val physicalOp = PhysicalOp
      .sourcePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecWithCode(code, "python"))
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
      .withLocationPreference(Option.empty)
      .withParallelizable(false) // CSV reader is single-threaded for now

    physicalOp
  }

  /** Creates a self-contained Python UDFSourceOperator script. */
  private def generatePythonCode(
      fullPath: String,
      delimiterOpt: Option[String],
      header: Boolean
  ): String = {
    val schemaJson = schemaToJson(sourceSchema())
    val schemaJsonEsc = schemaJson.replace("\\", "\\\\").replace("\"", "\\\"")

    val sepKwarg =
      delimiterOpt.filter(_ != ",").map(d => s"sep='$d'").getOrElse("")
    val headerKwarg =
      if (!header) "header=None" else ""
    val kwargs = Seq(sepKwarg, headerKwarg).filter(_.nonEmpty).mkString(", ")

    val userJwtToken =
      Option(System.getProperty("USER_JWT_TOKEN"))
        .orElse(Option(System.getenv("USER_JWT_TOKEN")))
        .getOrElse("")
    val tokenLine =
      if (userJwtToken.nonEmpty)
        s"""os.environ["USER_JWT_TOKEN"] = "${userJwtToken}""""
      else ""

    s"""
       |from pytexera import *
       |import os, json
       |
       |$tokenLine
       |
       |class GenerateOperator(UDFSourceOperator):
       |
       |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
       |        schema = json.loads("${schemaJsonEsc}")
       |        doc    = DatasetFileDocument("$fullPath")
       |        table  = doc.read_as_table(schema=schema, $kwargs)
       |
       |        for _, row in table.iterrows():
       |            yield row.to_dict()
       |""".stripMargin
  }

  // ------------- static operator metadata -------------------
  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Intelligent CSV Reader",
      "User-defined (Python) CSV source that streams rows downstream",
      OperatorGroupConstants.PYTHON_GROUP,
      List.empty, // source → no inputs
      List(OutputPort()), // single output
      supportReconfiguration = true
    )
  }

  override def sourceSchema(): Schema = {
    cachedSchema.getOrElse {
      val sch = computeSchema() // moved original body into helper
      cachedSchema = Some(sch)
      sch
    }
  }

  private def computeSchema(): Schema = {
    if (customDelimiter.isEmpty || !fileResolved()) return null

    val stream = DocumentFactory.openReadonlyDocument(new URI(fileName.get)).asInputStream()
    val inputReader = new InputStreamReader(stream, fileEncoding.getCharset)

    val csvFormat = new CsvFormat()
    csvFormat.setDelimiter(customDelimiter.get.charAt(0))
    csvFormat.setLineSeparator("\n")

    val csvSetting = new CsvParserSettings()
    csvSetting.setMaxCharsPerColumn(-1)
    csvSetting.setFormat(csvFormat)
    csvSetting.setHeaderExtractionEnabled(hasHeader)
    csvSetting.setNullValue("")

    val parser = new CsvParser(csvSetting)
    parser.beginParsing(inputReader)

    val dataBuf = scala.collection.mutable.ArrayBuffer.empty[Array[String]]
    val maxRows =
      if (readAll) Int.MaxValue else limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    val step = math.max(sampleEveryN, 1)

    var row: Array[String] = null
    var idx = 0
    while ({ row = parser.parseNext(); row != null && dataBuf.size < maxRows }) {
      if (readAll || idx % step == 0) dataBuf += row
      idx += 1
    }
    parser.stopParsing()
    inputReader.close()

    val attrTypes: Array[AttributeType] =
      inferSchemaFromRows(dataBuf.iterator.asInstanceOf[Iterator[Array[Any]]])

    val header: Array[String] =
      if (hasHeader)
        Option(parser.getContext.headers())
          .getOrElse((1 to attrTypes.length).map(i => s"column-$i").toArray)
      else
        (1 to attrTypes.length).map(i => s"column-$i").toArray

    header.indices.foldLeft(Schema()) { (schema, i) =>
      schema.add(header(i), attrTypes(i))
    }
  }
}
