package edu.uci.ics.texera.workflow.operators.source.scan.json
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters.asScalaIteratorConverter;

object JSONUtil {
  @throws[RuntimeException]
  def parseJSON(
      node: JsonNode,
      flatten: Boolean,
      parentName: String = ""
  ): Map[String, String] = {
    var result = Map[String, String]()
    if (node.isObject)
      for (key <- node.fieldNames().asScala) {
        val child: JsonNode = node.get(key)
        val absoluteKey = (if (parentName.nonEmpty) parentName + "." else "") + key
        if (flatten && (child.isObject || child.isArray)) {
          result = result ++ parseJSON(child, flatten, absoluteKey)
        } else if (child.isValueNode) {
          result = result + (absoluteKey -> child.asText())
        } else {
          // do nothing
        }
      }
    else if (node.isArray) {
      for ((child, i) <- node.elements().asScala.zipWithIndex) {
        result = result ++ parseJSON(child, flatten, parentName + (i + 1))
      }
    }
    result
  }

}
