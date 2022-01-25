package edu.uci.ics.texera.workflow.common.tuple

import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.{
  inferSchemaFromRows,
  parseField
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap
import org.bson.Document

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object TupleUtils {

  def tuple2json(tuple: Tuple): String = {
    tuple.asKeyValuePairJson().toString
  }

  def json2tuple(json: String): Tuple = {
    var fieldNames = Set[String]()

    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()

    val root: JsonNode = objectMapper.readTree(json)
    if (root.isObject) {
      val fields: Map[String, String] = JSONToMap(root)
      fieldNames = fieldNames.++(fields.keySet)
      allFields += fields
    }

    val sortedFieldNames = fieldNames.toList

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

    val schema = Schema.newBuilder
      .add(
        sortedFieldNames.indices
          .map(i => new Attribute(sortedFieldNames(i), attributeTypes(i)))
          .asJava
      )
      .build

    try {
      val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
      val data = JSONToMap(objectMapper.readTree(json))

      for (fieldName <- schema.getAttributeNames.asScala) {
        if (data.contains(fieldName)) {
          fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
        } else {
          fields += null
        }
      }
      Tuple.newBuilder(schema).addSequentially(fields.toArray).build()
    } catch {
      case e: Exception => throw e
    }
  }

  def document2Tuple(doc: Document, schema: Schema): Tuple = {
    val builder = Tuple.newBuilder(schema)
    schema.getAttributes.forEach(attr =>
      builder.add(attr, parseField(doc.get(attr.getName), attr.getType))
    )
    builder.build()
  }

}
