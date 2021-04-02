package edu.uci.ics.texera.workflow.operators.source.scan.json

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.parseJSON
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileReader, IOException}
import scala.collection.JavaConverters._

class JSONLScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(required = true)
  var flatten: Boolean = false

  fileTypeName = Option("JSONL")

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

  /**
    * Infer Texera.Schema based on the top few lines of data.
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
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
