package edu.uci.ics.texera.workflow.operators.scan

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
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

import java.io.{BufferedReader, File, FileReader, IOException}
import java.nio.charset.Charset
import java.util.Collections.singletonList
import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.util.control.Exception._

class CSVScanSourceOpDesc extends SourceOperatorDescriptor {

  @JsonIgnore
  val INFER_READ_LIMIT: Int = 100

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

  @JsonIgnore
  var filePath: Option[String] = None

  @throws[IOException]
  override def operatorExecutor: CSVScanSourceOpExecConfig = {
    // fill in default values
    if (delimiter.get.isEmpty)
      delimiter = Option(",")

    filePath match {
      case Some(path) =>
        val headerLine: String =
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
      "Scan data from a CSV file",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  }

  @throws[IOException]
  override def sourceSchema(): Schema = {
    if (filePath.isEmpty) return null

    val headerLine: String =
      Files.asCharSource(new File(filePath.get), Charset.defaultCharset).readFirstLine
    inferSchema(headerLine)

  }

  override def setContext(workflowContext: WorkflowContext): Unit = {
    super.setContext(workflowContext)

    if (context.userID.isDefined)
      // if context has a valid user ID, the fileName will be a file name,
      // resolve fileName to be the actual file path.
      filePath = Option(
        UserFileUtils.getFilePath(context.userID.get.toString, fileName.get).toString
      )
    else
      // otherwise, the fileName will be inputted by user, which is the filePath.
      filePath = fileName

  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    * @param headerLine usually the first line of the CSV file which contains table headers.
    * @return Texera.Schema build for this operator
    */
  private def inferSchema(headerLine: String): Schema = {
    if (delimiter.isEmpty) return null

    val headers: Array[String] = headerLine.split(delimiter.get)
    val attributeTypeList: Array[AttributeType] =
      Array.fill[AttributeType](headers.length)(AttributeType.INTEGER)

    val reader = new BufferedReader(new FileReader(filePath.get))

    if (hasHeader)
      reader.readLine()
    var i = 0

    // TODO: real CSV may contain multi-line values. Need to handle multi-line values correctly.
    var line: String = reader.readLine()
    while (line != null && i < INFER_READ_LIMIT) {
      inferRow(attributeTypeList, line.split(delimiter.get))
      i += 1
      line = reader.readLine()
    }
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

  /**
    * Infers field types of a given row of data. The given attributeTypeList will be updated
    * through each iteration of row inference, to contain the must accurate inference.
    * @param attributeTypeList AttributeTypes that being passed to each iteration.
    * @param fields data fields to be parsed, originally as String fields
    * @return
    */
  private def inferRow(
      attributeTypeList: Array[AttributeType],
      fields: Array[String]
  ): Unit = {
    for (i <- fields.indices) {
      attributeTypeList.update(i, inferField(attributeTypeList.apply(i), fields.apply(i)))
    }
  }

  private def inferField(attributeType: AttributeType, fieldValue: String): AttributeType = {
    attributeType match {
      case AttributeType.STRING  => tryParseString()
      case AttributeType.BOOLEAN => tryParseBoolean(fieldValue)
      case AttributeType.DOUBLE  => tryParseDouble(fieldValue)
      case AttributeType.LONG    => tryParseLong(fieldValue)
      case AttributeType.INTEGER => tryParseInteger(fieldValue)
      case _                     => tryParseString()
    }
  }

  private def tryParseInteger(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toInt match {
      case Some(_) => AttributeType.INTEGER
      case None    => tryParseLong(fieldValue)
    }
  }

  private def tryParseLong(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toLong match {
      case Some(_) => AttributeType.LONG
      case None    => tryParseDouble(fieldValue)
    }
  }

  private def tryParseDouble(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toDouble match {
      case Some(_) => AttributeType.DOUBLE
      case None    => tryParseBoolean(fieldValue)
    }
  }
  private def tryParseBoolean(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toBoolean match {
      case Some(_) => AttributeType.BOOLEAN
      case None    => tryParseString()
    }
  }

  private def tryParseString(): AttributeType = {
    AttributeType.STRING
  }

}
