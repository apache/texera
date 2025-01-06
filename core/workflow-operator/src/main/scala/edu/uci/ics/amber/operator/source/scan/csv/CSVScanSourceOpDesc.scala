package edu.uci.ics.amber.operator.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

import java.io.{IOException, InputStreamReader}
import java.net.URI

class CSVScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var customDelimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  fileTypeName = Option("CSV")

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    // fill in default values
    if (customDelimiter.isEmpty || customDelimiter.get.isEmpty) {
      customDelimiter = Option(",")
    }

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpExec",
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
    if (customDelimiter.isEmpty || !fileResolved()) {
      return null
    }
    val stream = DocumentFactory.newReadonlyDocument(new URI(fileName.get)).asInputStream()
    val inputReader =
      new InputStreamReader(stream, fileEncoding.getCharset)

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

    var data: Array[Array[String]] = Array()
    val readLimit = limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    for (i <- 0 until readLimit) {
      val row = parser.parseNext()
      if (row != null) {
        data = data :+ row
      }
    }
    parser.stopParsing()
    inputReader.close()

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      data.iterator.asInstanceOf[Iterator[Array[Any]]]
    )
    val header: Array[String] =
      if (hasHeader) parser.getContext.headers()
      else (1 to attributeTypeList.length).map(i => "column-" + i).toArray

    header.indices.foldLeft(Schema()) { (schema, i) =>
      schema.add(header(i), attributeTypeList(i))
    }

  }

}
