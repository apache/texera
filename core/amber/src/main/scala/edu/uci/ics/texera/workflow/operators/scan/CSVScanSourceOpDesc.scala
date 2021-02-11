package edu.uci.ics.texera.workflow.operators.scan

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.io.Files
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileUtils
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.codehaus.jackson.map.annotate.JsonDeserialize

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.util.Collections.singletonList
import scala.collection.JavaConverters._
import scala.collection.immutable.List
class CSVScanSourceOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("File")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var fileName: Option[String] = None

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

    fileName match {
      case Some(path) =>
        val headerLine =
          Files.asCharSource(new File(path), Charset.defaultCharset).readFirstLine

        new CSVScanSourceOpExecConfig(
          this.operatorIdentifier,
          Constants.defaultNumWorkers,
          path,
          delimiter.get.charAt(0),
          this.inferSchema(headerLine),
          hasHeader
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "CSV File Scan",
      "Scan data from a local CSV file",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(new OutputPort(""))).toList
    )
  }

  @throws[IOException]
  override def sourceSchema(): Schema = {
    if (fileName.isEmpty) return null

    val headerLine =
      Files.asCharSource(new File(fileName.get), Charset.defaultCharset).readFirstLine
    inferSchema(headerLine)

  }

  private def inferSchema(headerLine: String): Schema = {
    if (delimiter.isEmpty) return null

    val headers: Array[String] = headerLine.split(delimiter.get)
    Schema.newBuilder
      .add(
        if (hasHeader)
          headers
            .map((c: String) => { c.trim })
            .map((c: String) => new Attribute(c, AttributeType.STRING))
            .toIterable
            .asJava
        else
          headers.indices
            .map((i: Int) => new Attribute("column" + i, AttributeType.STRING))
            .asJava
      )
      .build

  }

  override def setContext(workflowContext: WorkflowContext): Unit = {
    super.setContext(workflowContext)
    if (context.userID.isDefined)
      fileName = Option(
        UserFileUtils.getFilePath(context.userID.get.toString, fileName.get).toString
      )
  }
}
