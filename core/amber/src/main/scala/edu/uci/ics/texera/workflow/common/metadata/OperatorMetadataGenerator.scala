package edu.uci.ics.texera.workflow.common.metadata

import java.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.introspect.AnnotatedClass
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig.html5EnabledSchema
import com.kjetland.jackson.jsonSchema.{JsonSchemaConfig, JsonSchemaDraft, JsonSchemaGenerator}
import edu.uci.ics.texera.workflow.common.TexeraUtils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor
import edu.uci.ics.texera.workflow.operators.aggregate.AverageOpDesc
import edu.uci.ics.texera.workflow.operators.localscan.LocalCsvFileScanOpDesc

import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaIterator

case class TexeraOperatorInfo(
    userFriendlyName: String,
    operatorDescription: String,
    operatorGroupName: String,
    numInputPorts: Int,
    numOutputPorts: Int
)

case class OperatorMetadata(
    operatorType: String,
    jsonSchema: JsonNode,
    additionalMetadata: TexeraOperatorInfo
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

  def main(args: Array[String]): Unit = {
    // run this if you want to check the json schema generated for an operator descriptor
    // replace the argument with the class of your operator descriptor
    println(generateOperatorJsonSchema(classOf[LocalCsvFileScanOpDesc]).toPrettyString)
  }

  val texeraSchemaGeneratorConfig: JsonSchemaConfig = html5EnabledSchema.copy(
    defaultArrayFormat = None,
    jsonSchemaDraft = JsonSchemaDraft.DRAFT_07
  )

  val jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, texeraSchemaGeneratorConfig)

  // a map from a Texera Operator Descriptor's class to its operatorType string value
  val operatorTypeMap: Map[Class[_ <: TexeraOperatorDescriptor], String] = {
    // find all the operator type declarations in PredicateBase annotation
    val types = objectMapper.getSubtypeResolver.collectAndResolveSubtypesByClass(
      objectMapper.getDeserializationConfig,
      AnnotatedClass.construct(objectMapper.constructType(classOf[TexeraOperatorDescriptor]),
        objectMapper.getDeserializationConfig)
    )
    JavaConverters.asScalaBuffer(new util.ArrayList[NamedType](types))
      .filter(t => t.getType != null && t.getName != null)
      .map(t => (t.getType.asInstanceOf[Class[_ <: TexeraOperatorDescriptor]], t.getName)).toMap
  }

  val allOperatorMetadata: AllOperatorMetadata = generateAllOperatorMetadata()

  def generateAllOperatorMetadata(): AllOperatorMetadata = {
    AllOperatorMetadata(
      operatorTypeMap.keys.map(generateOperatorMetadata).toList,
      OperatorGroupConstants.OperatorGroupOrderList)
  }

  def generateOperatorMetadata(opDescClass: Class[_ <: TexeraOperatorDescriptor]): OperatorMetadata = {
    if (!operatorTypeMap.contains(opDescClass))
      throw new RuntimeException("Texera Operator Descriptor class " + opDescClass.toString + " is not registered in TexeraOperatorDescription class")

    // find the operatorType of the predicate class
    val operatorType = operatorTypeMap(opDescClass)

    // generate json schema for operator properties
    val jsonSchema = generateOperatorJsonSchema(opDescClass)

    // generate texera operator info
    val texeraOperatorInfo = opDescClass.getConstructor().newInstance().texeraOperatorInfo

    OperatorMetadata(operatorType, jsonSchema, texeraOperatorInfo)
  }

  def generateOperatorJsonSchema(opDescClass: Class[_ <: TexeraOperatorDescriptor]): JsonNode = {
    val jsonSchema = jsonSchemaGenerator.generateJsonSchema(opDescClass).asInstanceOf[ObjectNode]
    // remove operatorID from json schema
    jsonSchema.get("properties").asInstanceOf[ObjectNode].remove("operatorID")
    // remove operatorType from json schema
    jsonSchema.get("properties").asInstanceOf[ObjectNode].remove("operatorType")
    // remove operatorType from required list
    val operatorTypeIndex = asScalaIterator(jsonSchema.get("required").asInstanceOf[ArrayNode].elements())
      .indexWhere(p => p.asText().equals("operatorType"))
    jsonSchema.get("required").asInstanceOf[ArrayNode].remove(operatorTypeIndex)
    // remove "title" for the operator - frontend uses userFriendlyName to show operator title
    jsonSchema.remove("title")
    jsonSchema
  }

}
