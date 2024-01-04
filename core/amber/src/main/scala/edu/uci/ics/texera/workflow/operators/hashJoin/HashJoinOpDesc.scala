package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.workflow.common.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PartitionInfo}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "buildAttributeName": {
      "const": {
        "$data": "probeAttributeName"
      }
    }
  }
}
""")
class HashJoinOpDesc[K] extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Left Input")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Right Input")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonProperty(required = true, defaultValue = "inner")
  @JsonSchemaTitle("Join Type")
  @JsonPropertyDescription("select the join type to execute")
  var joinType: JoinType = JoinType.INNER

  override def getPhysicalOp(
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    val partitionRequirement = List(
      Option(HashPartition(List(operatorSchemaInfo.inputSchemas(0).getIndex(buildAttributeName)))),
      Option(HashPartition(List(operatorSchemaInfo.inputSchemas(1).getIndex(probeAttributeName))))
    )

    val joinDerivePartition: List[PartitionInfo] => PartitionInfo = inputPartitions => {
      val buildPartition = inputPartitions(0).asInstanceOf[HashPartition]
      val probePartition = inputPartitions(1).asInstanceOf[HashPartition]

      val buildAttrIndex = operatorSchemaInfo.inputSchemas(0).getIndex(buildAttributeName)
      val probAttrIndex = operatorSchemaInfo.inputSchemas(1).getIndex(probeAttributeName)

      assert(buildPartition.hashColumnIndices.contains(buildAttrIndex))
      assert(probePartition.hashColumnIndices.contains(probAttrIndex))

      // mapping from build/probe schema index to the final output schema index
      val schemaMappings = getOutputSchemaInternal(operatorSchemaInfo.inputSchemas)
      val buildMapping = schemaMappings._2
      val probeMapping = schemaMappings._3

      val outputHashIndices = buildPartition.hashColumnIndices.flatMap(i => buildMapping.get(i)) ++
        probePartition.hashColumnIndices.flatMap(i => probeMapping.get(i))

      assert(outputHashIndices.nonEmpty)

      HashPartition(outputHashIndices)
    }

    PhysicalOp
      .oneToOnePhysicalOp(
        executionId,
        operatorIdentifier,
        OpExecInitInfo(_ =>
          new HashJoinOpExec[K](
            buildAttributeName,
            probeAttributeName,
            joinType,
            operatorSchemaInfo
          )
        )
      )
      .copy(
        inputPorts = operatorInfo.inputPorts,
        outputPorts = operatorInfo.outputPorts,
        partitionRequirement = partitionRequirement,
        derivePartition = joinDerivePartition,
        blockingInputs = List(0),
        dependency = Map(1 -> 0)
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  /*
    returns a triple containing
    1: the output schema
    2: a mapping of left  input attribute index to output attribute index
    3: a mapping of right input attribute index to output attribute index

    For example, Left(id, l1, l2) joins Right(id, r1, r2) on id:
    1. output schema: (id, l1, l2, r1, r2)
    2. left  mapping: (0->0, 1->1, 2->2)
    3. right mapping: (0->0, 1->3, 1->4)
   */
  def getOutputSchemaInternal(schemas: Array[Schema]): (Schema, Map[Int, Int], Map[Int, Int]) = {
    Preconditions.checkArgument(schemas.length == 2)
    val builder = Schema.newBuilder()
    val buildSchema = schemas(0)
    val probeSchema = schemas(1)
    builder.add(buildSchema).removeIfExists(probeAttributeName)
    if (probeAttributeName.equals(buildAttributeName)) {
      probeSchema.getAttributes.foreach(attr => {
        val attributeName = attr.getName
        if (
          builder.build().containsAttribute(attributeName) && attributeName != probeAttributeName
        ) {
          // appending 1 to the output of Join schema in case of duplicate attributes in probe and build table
          val originalAttrName = attr.getName
          var attributeName = originalAttrName
          var suffix = 1
          while (builder.build().containsAttribute(attributeName)) {
            attributeName = s"$originalAttrName#@$suffix"
            suffix += 1
          }
          builder.add(new Attribute(attributeName, attr.getType))
        } else {
          builder.add(attr)
        }
      })
      val leftSchemaMapping =
        buildSchema.getAttributeNamesScala.zipWithIndex
          .filter(p => p._1 != buildAttributeName)
          .map(p => p._2)
          .zipWithIndex
          .map(p => (p._1, p._2))
          .toMap

      val rightSchemaMapping = probeSchema.getAttributesScala.indices
        .map(i => (i, i + buildSchema.getAttributes.size() - 1))
        .toMap

      (builder.build(), leftSchemaMapping, rightSchemaMapping)
    } else {
      probeSchema.getAttributes
        .forEach(attr => {
          val originalAttrName = attr.getName
          var attributeName = originalAttrName
          if (builder.build().containsAttribute(attributeName)) {
            var suffix = 1
            while (builder.build().containsAttribute(attributeName)) {
              attributeName = s"$originalAttrName#@$suffix"
              suffix += 1
            }
            builder.add(new Attribute(attributeName, attr.getType))
          } else if (!attributeName.equalsIgnoreCase(probeAttributeName)) {
            builder.add(attr)
          }
        })

      val leftSchemaMapping =
        buildSchema.getAttributeNamesScala.indices.map(i => (i, i)).toMap

      val rightSchemaMapping =
        probeSchema.getAttributeNamesScala.zipWithIndex
          .filter(p => p._1 != probeAttributeName)
          .map(p => p._2)
          .zipWithIndex
          .map(p => (p._1, p._2 + +buildSchema.getAttributes.size()))
          .toMap

      (builder.build(), leftSchemaMapping, rightSchemaMapping)
    }
  }

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    getOutputSchemaInternal(schemas)._1
  }
}
