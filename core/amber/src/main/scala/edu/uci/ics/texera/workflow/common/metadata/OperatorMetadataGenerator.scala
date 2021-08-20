package edu.uci.ics.texera.workflow.common.metadata

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig.html5EnabledSchema
import com.kjetland.jackson.jsonSchema.{JsonSchemaConfig, JsonSchemaDraft, JsonSchemaGenerator}
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc

import java.util
import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaIterator

case class InputPort(
    displayName: String = null,
    allowMultiInputs: Boolean = false
)

case class OutputPort(
    displayName: String = null
)

case class OperatorInfo(
    userFriendlyName: String,
    operatorDescription: String,
    operatorGroupName: String,
    inputPorts: List[InputPort],
    outputPorts: List[OutputPort]
)

case class OperatorMetadata(
    operatorType: String,
    jsonSchema: JsonNode,
    additionalMetadata: OperatorInfo
)

case class GroupInfo(
    groupName: String,
    groupOrder: Int
)

case class AllOperatorMetadata(
    operators: List[OperatorMetadata],
    groups: List[GroupInfo]
)

/**
  * Generates the metadata of a Texera Operator Descriptor for the frontend.
  * The type definitions correspond to "workspace/types/operator-schema.interface.ts" in frontend.
  */
object OperatorMetadataGenerator {

  val texeraSchemaGeneratorConfig: JsonSchemaConfig = html5EnabledSchema.copy(
    useOneOfForOption = false,
    useOneOfForNullables = false,
    useNullableForOption = true,
    useNullableForNullables = true,
    defaultArrayFormat = None,
    jsonSchemaDraft = JsonSchemaDraft.DRAFT_07
  )
  val jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, texeraSchemaGeneratorConfig)
  // a map from a Texera Operator Descriptor's class to its operatorType string value
  val operatorTypeMap: Map[Class[_ <: OperatorDescriptor], String] = {
    // find all the operator type declarations in PredicateBase annotation
    val types = objectMapper.getSubtypeResolver.collectAndResolveSubtypesByClass(
      objectMapper.getDeserializationConfig,
      AnnotatedClassResolver.resolveWithoutSuperTypes(
        objectMapper.getDeserializationConfig,
        objectMapper.constructType(classOf[OperatorDescriptor]).getRawClass
      )
    )
    JavaConverters
      .asScalaBuffer(new util.ArrayList[NamedType](types))
      .filter(t => t.getType != null && t.getName != null)
      .map(t => (t.getType.asInstanceOf[Class[_ <: OperatorDescriptor]], t.getName))
      .toMap
  }
  val allOperatorMetadata: AllOperatorMetadata = generateAllOperatorMetadata()

  def main(args: Array[String]): Unit = {
    // run this if you want to check the json schema generated for an operator descriptor
    // replace the argument with the class of your operator descriptor
    println(generateOperatorJsonSchema(classOf[CSVScanSourceOpDesc]).toPrettyString)
  }

  def generateOperatorJsonSchema(opDescClass: Class[_ <: OperatorDescriptor]): JsonNode = {
    val jsonSchema = jsonSchemaGenerator.generateJsonSchema(opDescClass).asInstanceOf[ObjectNode]
    // remove operatorID from json schema
    jsonSchema.get("properties").asInstanceOf[ObjectNode].remove("operatorID")
    // remove operatorType from json schema
    jsonSchema.get("properties").asInstanceOf[ObjectNode].remove("operatorType")
    // remove operatorType from required list
    val operatorTypeIndex =
      asScalaIterator(jsonSchema.get("required").asInstanceOf[ArrayNode].elements())
        .indexWhere(p => p.asText().equals("operatorType"))
    jsonSchema.get("required").asInstanceOf[ArrayNode].remove(operatorTypeIndex)
    // remove "title" for the operator - frontend uses userFriendlyName to show operator title
    jsonSchema.remove("title")
    jsonSchema
  }

  def generateAllOperatorMetadata(): AllOperatorMetadata = {
    AllOperatorMetadata(
      operatorTypeMap.keys.map(generateOperatorMetadata).toList,
      OperatorGroupConstants.OperatorGroupOrderList
    )
  }

  def generateOperatorMetadata(opDescClass: Class[_ <: OperatorDescriptor]): OperatorMetadata = {
    if (!operatorTypeMap.contains(opDescClass))
      throw new RuntimeException(
        "Texera Operator Descriptor class " + opDescClass.toString + " is not registered in TexeraOperatorDescription class"
      )

    // find the operatorType of the predicate class
    val operatorType = operatorTypeMap(opDescClass)

    // generate json schema for operator properties
    val jsonSchema = generateOperatorJsonSchema(opDescClass)

    // generate texera operator info
    val texeraOperatorInfo = opDescClass.getConstructor().newInstance().operatorInfo

    OperatorMetadata(operatorType, jsonSchema, texeraOperatorInfo)
  }

}
