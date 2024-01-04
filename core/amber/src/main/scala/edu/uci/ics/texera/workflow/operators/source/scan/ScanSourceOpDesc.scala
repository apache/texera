package edu.uci.ics.texera.workflow.operators.source.scan

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileAccessResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.apache.commons.lang3.builder.EqualsBuilder
import org.jooq.types.UInteger

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer

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
    if (filePath.isEmpty) return null
    inferSchema()

  }

  override def setContext(workflowContext: WorkflowContext): Unit = {
    super.setContext(workflowContext)

    if (fileName.isEmpty) {
      throw new RuntimeException("no input file name")
    }

    if (getContext.userId.isDefined) {
      // if context has a valid user ID, the fileName will be in the following format:
      //    ownerName/fileName
      // resolve fileName to be the actual file path.
      val splitNames = fileName.get.split("/")
      filePath = UserFileAccessResource
        .getFilePath(
          email = splitNames.apply(0),
          fileName = splitNames.apply(1),
          getContext.userId.get,
          UInteger.valueOf(getContext.workflowId.id)
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
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  }

  def inferSchema(): Schema

  override def equals(that: Any): Boolean =
    EqualsBuilder.reflectionEquals(this, that, "context", "filePath")
}
