package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.model.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.model.tuple.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.storage.DocumentFactory
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{IOException, InputStreamReader}
import java.net.URI

class CSVScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
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
    if (customDelimiter.isEmpty || customDelimiter.get.isEmpty)
      customDelimiter = Option(",")

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) =>
          new CSVScanSourceOpExec(
            fileUri.get,
            fileEncoding,
            limit,
            offset,
            customDelimiter,
            hasHeader,
            schemaFunc = () => sourceSchema()
          )
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
    if (customDelimiter.isEmpty || fileUri.isEmpty) {
      return null
    }

    val stream = DocumentFactory.newReadonlyDocument(new URI(fileUri.get)).asInputStream()
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

    Schema
      .builder()
      .add(header.indices.map(i => new Attribute(header(i), attributeTypeList(i))))
      .build()
  }

}
