package edu.uci.ics.texera.workflow.operators.projection

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata._
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameList
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.jdk.CollectionConverters.asJavaIterableConverter

class ProjectionOpDesc extends MapOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attributes")
  @JsonPropertyDescription("a subset of column to keeps")
  @AutofillAttributeNameList
  val attributes: List[String] = List()

  override def operatorExecutor: OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(operatorIdentifier, _ => new ProjectionOpExec(attributes))
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Projection",
      "Keeps the column",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)

    Schema.newBuilder
      .add(schemas(0).getAttributes.filter(attr => attributes.contains(attr.getName)).asJava)
      .build
  }
}
