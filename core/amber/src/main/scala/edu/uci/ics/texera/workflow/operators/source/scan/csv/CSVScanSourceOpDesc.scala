package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.io.Files
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc
import org.codehaus.jackson.map.annotate.JsonDeserialize

import java.io.{BufferedReader, File, FileReader, IOException}
import java.nio.charset.Charset
import java.util.Collections.singletonList
import scala.collection.JavaConverters._
import scala.collection.immutable.List

class CSVScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonIgnore
  var headerLine: String = _

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var delimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  @throws[IOException]
  override def operatorExecutor: CSVScanSourceOpExecConfig = {
    // fill in default values
    if (delimiter.get.isEmpty)
      delimiter = Option(",")

    filePath match {
      case Some(path) =>
        headerLine = Files.asCharSource(new File(path), Charset.defaultCharset).readFirstLine

        new CSVScanSourceOpExecConfig(
          operatorIdentifier,
          Constants.defaultNumWorkers,
          path,
          inferSchema(),
          delimiter.get.charAt(0),
          hasHeader
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "CSV File Scan",
      "Scan data from a CSV file",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    if (delimiter.isEmpty) return null

    val headers: Array[String] = headerLine.split(delimiter.get)

    val reader = new BufferedReader(new FileReader(filePath.get))
    if (hasHeader)
      reader.readLine()

    // TODO: real CSV may contain multi-line values. Need to handle multi-line values correctly.

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader
        .lines()
        .iterator
        .asScala
        .take(INFER_READ_LIMIT)
        .map(line => line.split(delimiter.get).asInstanceOf[Array[Object]])
    )

    reader.close()

    // build schema based on inferred AttributeTypes
    Schema.newBuilder
      .add(
        if (hasHeader)
          headers.indices
            .map((i: Int) => new Attribute(headers.apply(i), attributeTypeList.apply(i)))
            .asJava
        else
          headers.indices
            .map((i: Int) => new Attribute("column-" + (i + 1), attributeTypeList.apply(i)))
            .asJava
      )
      .build
  }

}
