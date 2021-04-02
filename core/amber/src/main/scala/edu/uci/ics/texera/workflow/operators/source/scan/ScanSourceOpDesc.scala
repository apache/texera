package edu.uci.ics.texera.workflow.operators.source.scan
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileUtils
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.codehaus.jackson.map.annotate.JsonDeserialize

abstract class ScanSourceOpDesc extends SourceOperatorDescriptor {

  @JsonIgnore
  val INFER_READ_LIMIT: Int = 100
  @JsonProperty(required = true)
  @JsonSchemaTitle("File")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var fileName: Option[String] = None
  @JsonIgnore
  var filePath: Option[String] = None

  override def sourceSchema(): Schema = {
    if (filePath.isEmpty) return null
    inferSchema()

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

  def inferSchema(): Schema
}
