package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{File, IOException}
import scala.jdk.CollectionConverters.IterableHasAsJava

class ParallelCSVScanSourceOpDesc extends ScanSourceOpDesc {

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
    if (customDelimiter.get.isEmpty)
      customDelimiter = Option(",")

    filePath match {
      case Some(path) =>
        val totalBytes: Long = new File(path).length()

        PhysicalOp
          .sourcePhysicalOp(
            workflowId,
            executionId,
            operatorIdentifier,
            OpExecInitInfo((idx, _, operatorConfig) => {
              val workerCount = operatorConfig.workerConfigs.length
              // TODO: add support for limit
              // TODO: add support for offset
              val startOffset: Long = totalBytes / workerCount * idx
              val endOffset: Long =
                if (idx != workerCount - 1) totalBytes / workerCount * (idx + 1) else totalBytes
              new ParallelCSVScanSourceOpExec(
                this,
                startOffset,
                endOffset
              )
            })
          )
          .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
          .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
          .withParallelizable(true)

      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    if (customDelimiter.isEmpty) {
      return null
    }
    if (filePath.isEmpty) {
      return null
    }
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = customDelimiter.get.charAt(0)

    }
    var reader: CSVReader = CSVReader.open(filePath.get)(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    // reopen the file to read from the beginning
    reader = CSVReader.open(filePath.get)(CustomFormat)
    if (hasHeader)
      reader.readNext()

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .take(limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT))
        .map(seq => seq.toArray)
    )

    reader.close()

    // build schema based on inferred AttributeTypes
    Schema.newBuilder
      .add(
        firstRow.indices
          .map((i: Int) =>
            new Attribute(
              if (hasHeader) firstRow.apply(i) else "column-" + (i + 1),
              attributeTypeList.apply(i)
            )
          )
          .asJava
      )
      .build
  }

}
