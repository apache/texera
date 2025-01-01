package edu.uci.ics.amber.operator.source.scan.csvOld

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI

class CSVOldScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  var customDelimiter: Option[String] = Some(",")

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  fileTypeName = Option("CSVOld")

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    // fill in default values
    if (customDelimiter.get.isEmpty) {
      customDelimiter = Option(",")
    }
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.source.scan.csvOld.CSVOldScanSourceOpExec",
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
    // infer schema from the first few lines of the file
    val file = DocumentFactory.newReadonlyDocument(new URI(fileName.get)).asFile()
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = customDelimiter.get.charAt(0)
    }
    var reader: CSVReader =
      CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    // reopen the file to read from the beginning
    reader = CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)

    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)
    val endOffset =
      startOffset + limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .slice(startOffset, endOffset)
        .map(seq => seq.toArray)
    )

    reader.close()

    // build schema based on inferred AttributeTypes
    Schema
      .builder()
      .add(
        firstRow.indices
          .map((i: Int) =>
            new Attribute(
              if (hasHeader) firstRow.apply(i) else "column-" + (i + 1),
              attributeTypeList.apply(i)
            )
          )
      )
      .build()

  }

}
