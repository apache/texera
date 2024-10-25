package edu.uci.ics.texera.workflow.operators.source.scan

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.model.WorkflowContext
import edu.uci.ics.amber.engine.common.model.tuple.Schema
import edu.uci.ics.amber.engine.common.storage.DatasetFileDocument
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import org.apache.commons.lang3.builder.EqualsBuilder

import java.nio.file.Paths

abstract class ScanSourceOpDesc extends SourceOperatorDescriptor {

  /** in the case we do not want to read the entire large file, but only
    * the first a few lines of it to do the type inference.
    */
  @JsonIgnore
  var INFER_READ_LIMIT: Int = 100

  @JsonProperty(required = true)
  @JsonSchemaTitle("File")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var fileName: Option[String] = None

  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("File Encoding")
  @JsonPropertyDescription("decoding charset to use on input")
  var fileEncoding: FileDecodingMethod = FileDecodingMethod.UTF_8

  @JsonIgnore
  var filePath: Option[String] = None

  @JsonIgnore
  var datasetFile: Option[DatasetFileDocument] = None

  @JsonIgnore
  var fileTypeName: Option[String] = None

  @JsonProperty()
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("max output count")
  @JsonDeserialize(contentAs = classOf[Int])
  var limit: Option[Int] = None

  @JsonProperty()
  @JsonSchemaTitle("Offset")
  @JsonPropertyDescription("starting point of output")
  @JsonDeserialize(contentAs = classOf[Int])
  var offset: Option[Int] = None

  override def sourceSchema(): Schema = {
    if (filePath.isEmpty && datasetFile.isEmpty) return null
    inferSchema()
  }

  override def setContext(workflowContext: WorkflowContext): Unit = {
    super.setContext(workflowContext)

    if (fileName.isEmpty) {
      throw new RuntimeException("no input file name")
    }
    val datasetFilePathPrefix = "file://"
    if (fileName.get.startsWith(datasetFilePathPrefix)) {
      // filePath starts with file://, a datasetFileDesc will be initialized, which is the handle of reading file from the dataset
      datasetFile = Some(
        new DatasetFileDocument(Paths.get(fileName.get.substring(datasetFilePathPrefix.length)))
      )
    } else {
      // otherwise, the fileName will be inputted by user, which is the filePath.
      filePath = fileName
    }
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = s"${fileTypeName.get} File Scan",
      operatorDescription = s"Scan data from a ${fileTypeName.get} file",
      OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
  }

  def inferSchema(): Schema

  // get the source file descriptor from the fields
  // either the datasetFile or the filePath should be defined
  def determineFilePathOrDatasetFile(): (String, DatasetFileDocument) = {
    if (
      (datasetFile.isEmpty && filePath.isEmpty)
      || (datasetFile.isDefined && filePath.isDefined)
    ) {
      throw new RuntimeException("Source file descriptor is not set.")
    }
    if (datasetFile.isDefined) {
      val file = datasetFile.get
      (null, file)
    } else {
      val filepath = filePath.get
      (filepath, null)
    }
  }

  override def equals(that: Any): Boolean =
    EqualsBuilder.reflectionEquals(this, that, "context", "filePath")
}
