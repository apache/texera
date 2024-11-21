package edu.uci.ics.amber.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.noctordeser.NoCtorDeserModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.util.serde.{PortIdentityKeyDeserializer, PortIdentityKeySerializer}
import edu.uci.ics.amber.workflow.PortIdentity

import java.text.SimpleDateFormat
import scala.jdk.CollectionConverters.IteratorHasAsScala

object JSONUtils {

  /**
    * A singleton object for configuring the Jackson `ObjectMapper` to handle JSON serialization and deserialization
    * in Scala. This custom `ObjectMapper` is tailored for Scala, ensuring compatibility with Scala types
    * and specific serialization/deserialization settings.
    *
    * - Registers the `DefaultScalaModule` to ensure proper handling of Scala-specific types (e.g., `Option`, `Seq`).
    * - Registers the `NoCtorDeserModule` to handle deserialization of Scala classes that lack a default constructor,
    *   which is common in case classes.
    * - Registers the `SimpleModule` with pairs of serializer & deserializer to ensure proper handling of serializing
    *    and deserializing the PhysicalPlan
    * - Sets the serialization inclusion rules to exclude `null` and `absent` values:
    *   - `Include.NON_NULL`: Excludes fields with `null` values from the serialized JSON.
    *   - `Include.NON_ABSENT`: Excludes fields with `Option.empty` (or equivalent absent values) from serialization.
    * - Configures the date format for JSON serialization and deserialization:
    *   - The format is set to `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`, which follows the ISO-8601 standard for representing date and time,
    *     commonly used in JSON APIs, including millisecond precision and the UTC 'Z' suffix.
    *
    * This `ObjectMapper` provides a consistent way to serialize and deserialize JSON while adhering to Scala conventions
    * and handling common patterns like `Option` and case classes.
    */
  final val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new NoCtorDeserModule())
    .registerModule(
      new SimpleModule()
        .addKeySerializer(classOf[PortIdentity], new PortIdentityKeySerializer())
        .addKeyDeserializer(classOf[PortIdentity], new PortIdentityKeyDeserializer())
    )
    .setSerializationInclusion(Include.NON_NULL)
    .setSerializationInclusion(Include.NON_ABSENT)
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

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
