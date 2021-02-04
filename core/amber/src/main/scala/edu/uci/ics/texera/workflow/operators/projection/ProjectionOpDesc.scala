package edu.uci.ics.texera.workflow.operators.projection

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.metadata._
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameList
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer

class ProjectionOpDesc extends MapOpDesc {

  @JsonProperty(value = "attributes", required = true)
  @JsonPropertyDescription("a subset of column to keeps")
  @AutofillAttributeNameList
  val attributes: List[String] = List[String]()

  override def operatorExecutor: OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(operatorIdentifier, (_: Any) => new ProjectionOpExec(this))
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Projection",
      "Keeps the column",
      OperatorGroupConstants.UTILITY_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    val builder = Schema.newBuilder
    schemas(0).getAttributes.forEach((attr: Attribute) => { builder.add(attr) })
    builder.build()
  }
}
