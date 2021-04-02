package edu.uci.ics.texera.workflow.operators.source.scan.json

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode
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
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.parseJSON
import org.codehaus.jackson.map.annotate.JsonDeserialize

import java.io.{BufferedReader, FileReader, IOException}
import java.util.Collections.singletonList
import scala.collection.JavaConverters._
import scala.collection.immutable.List

class JSONLScanSourceOpDesc extends SourceOperatorDescriptor {

  @JsonIgnore
  val INFER_READ_LIMIT: Int = 100

  @JsonProperty(required = true)
  @JsonSchemaTitle("File")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var fileName: Option[String] = None

  @JsonProperty(required = true)
  var flatten: Boolean = false

  @JsonIgnore
  var filePath: Option[String] = None

  @throws[IOException]
  override def operatorExecutor: JSONLScanSourceOpExecConfig = {
    // fill in default values

    filePath match {
      case Some(path) =>
        new JSONLScanSourceOpExecConfig(
          operatorIdentifier,
          Constants.defaultNumWorkers,
          path,
          inferSchema(),
          flatten
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "JSONL File Scan",
      "Scan data from a JSONL file",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  }

  @throws[IOException]
  override def sourceSchema(): Schema = {
    if (filePath.isEmpty) return null
    else println("This is " + filePath)
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

  /**
    * Infer Texera.Schema based on the top few lines of data.
    * @return Texera.Schema build for this operator
    */
  private def inferSchema(): Schema = {
    val reader = new BufferedReader(new FileReader(filePath.get))
    var fields = Set[String]()

    var line: String = null
    var count: Int = 0
    while ({
      line = reader.readLine()
      count += 1
      line
    } != null && count < INFER_READ_LIMIT) {
      val root: JsonNode = objectMapper.readTree(line)
      if (root.isObject) {
        fields = fields.++(parseJSON(root, flatten = flatten).keySet)
      }
    }
    reader.close()

    // TODO: use actual infer schema.
    Schema.newBuilder
      .add(
        fields.toList.sorted.map((field: String) => new Attribute(field, AttributeType.ANY)).asJava
      )
      .build
  }

}
