package edu.uci.ics.texera.workflow.operators.source.scan.json
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object JSONUtil {

  /**
    * this method helps convert JSON into a key-value Map. By default it will only
    * take the first level attributes of the JSON object, and ignore nested objects
    * and arrays. For example:
    * input JSON {"A" : "a", "B": 1, "C": 2.3, "D" :{"some":"object"}, "E": ["1", "2"]}
    * will be converted to Map[String, String]{"A" : "a", "B": "1", "C": "2.3"}.
    *
    * If flatten mode is enabled, then the nested objects and arrays will be converted
    * to map recursively. The key will be the `parentName[index].childName`. For example:
    * input JSON {"A" : "a", "B": 1, "C": 2.3, "D" :{"some":"object"}, "E": ["X", "Y"]}
    * will be converted to Map[String, String]{"A" : "a", "B": "1", "C": "2.3",
    * "D.some":"object", "E1":"X", "E2":"Y"}.
    *
    * @param node the JSONNode to convert.
    * @param flatten a boolean to toggle flatten mode.
    * @param parentName the parent's name to pass into children's naming conversion.
    * @return a Map[String, String] of all the key value pairs from the given JSONNode.
    */
  def JSONToMap(
      node: JsonNode,
      flatten: Boolean = false,
      parentName: String = ""
  ): Map[String, String] = {
    var result = Map[String, String]()
    if (node.isObject) {
      for (key <- node.fieldNames().asScala) {
        val child: JsonNode = node.get(key)
        val absoluteKey = (if (parentName.nonEmpty) parentName + "." else "") + key
        if (flatten && (child.isObject || child.isArray)) {
          result = result ++ JSONToMap(child, flatten, absoluteKey)
        } else if (child.isValueNode) {
          result = result + (absoluteKey -> child.asText())
        } else {
          // do nothing
        }
      }
    } else if (node.isArray) {
      for ((child, i) <- node.elements().asScala.zipWithIndex) {
        result = result ++ JSONToMap(child, flatten, parentName + (i + 1))
      }
    }
    result
  }

}
