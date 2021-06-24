package edu.uci.ics.texera.workflow.operators.source.scan.json

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileReader, IOException}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
class JSONLScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(required = true)
  @JsonPropertyDescription("flatten nested objects and arrays")
  var flatten: Boolean = false

  fileTypeName = Option("JSONL")

  @throws[IOException]
  override def operatorExecutor(
      operatorSchemaInfo: OperatorSchemaInfo
  ): JSONLScanSourceOpExecConfig = {
    filePath match {
      case Some(_) =>
        new JSONLScanSourceOpExecConfig(
          operatorIdentifier,
          this
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    *
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    val reader = new BufferedReader(new FileReader(filePath.get))
    var fieldNames = Set[String]()
    var fields: Map[String, String] = null
    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()


    val startOffset = offset.getOrElse(INFER_READ_LIMIT).asInstanceOf[Int]
    val endOffset = startOffset + limit.getOrElse(INFER_READ_LIMIT).asInstanceOf[Int].min(INFER_READ_LIMIT)
    reader.lines().iterator().asScala.slice(startOffset, endOffset).foreach(
      line => {
        val root: JsonNode = objectMapper.readTree(line)
        if (root.isObject) {
          fields = JSONToMap(root, flatten = flatten)
          fieldNames = fieldNames.++(fields.keySet)
          allFields += fields
        }
      }
    )

    val sortedFieldNames = fieldNames.toList.sorted
    reader.close()

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames) {
        if (fields.contains(fieldName)) {
          result += fields(fieldName)
        } else {
          result += null
        }
      }
      result.toArray
    }))

    Schema.newBuilder
      .add(
        sortedFieldNames.indices
          .map(i => new Attribute(sortedFieldNames(i), attributeTypes(i)))
          .asJava
      )
      .build
  }

}
