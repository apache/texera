package edu.uci.ics.texera.workflow.operators.source.scan.json
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters.asScalaIteratorConverter;

object JSONUtil {
  @throws[RuntimeException]
  def flattenJSON(node: JsonNode, parentName: String = ""): Map[String, String] = {
    var result = Map[String, String]()
    if (node.isObject)
      for (key <- node.fieldNames().asScala) {
        val child: JsonNode = node.get(key)
        val absoluteKey = (if (parentName.nonEmpty) parentName + "." else "") + key
        if (child.isObject || child.isArray) {
          result = result ++ flattenJSON(child, absoluteKey)
        } else if (child.isValueNode) {
          result = result + (absoluteKey -> child.asText())
        } else {
          // should never reach here
          throw new RuntimeException("Unexpected type of JSONNode")
        }
      }
    else if (node.isArray) {
      for ((child, i) <- node.elements().asScala.zipWithIndex) {
        result = result ++ flattenJSON(child, parentName + (i + 1))
      }
    }
    result
  }

}
