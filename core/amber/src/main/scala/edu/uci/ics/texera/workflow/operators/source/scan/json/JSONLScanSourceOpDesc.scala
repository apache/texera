package edu.uci.ics.texera.workflow.operators.source.scan.json

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileReader, IOException}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.asJavaIterableConverter

class JSONLScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(required = true)
  @JsonPropertyDescription("flatten nested objects and arrays")
  var flatten: Boolean = false

  fileTypeName = Option("JSONL")

  @throws[IOException]
  override def operatorExecutor(
      inputSchemas: Array[Schema],
      outputSchema: Schema
  ): JSONLScanSourceOpExecConfig = {
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
    var fieldNames = Set[String]()
    var fields: Map[String, String] = null
    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()
    var line: String = null
    var count: Int = 0
    while ({
      line = reader.readLine()
      count += 1
      line
    } != null && count <= INFER_READ_LIMIT) {
      val root: JsonNode = objectMapper.readTree(line)
      if (root.isObject) {
        fields = JSONToMap(root, flatten = flatten)
        fieldNames = fieldNames.++(fields.keySet)
        allFields += fields
      }
    }
    val sortedFieldNames = fieldNames.toList.sorted
    reader.close()

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames)
        if (fields.contains(fieldName))
          result += fields(fieldName)
        else
          result += null
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
