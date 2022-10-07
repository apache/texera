package edu.uci.ics.texera.workflow.operators.projection

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.metadata._
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.JavaConverters._

class ProjectionOpDesc extends MapOpDesc {

  var attributes: List[AttributeUnit] = List()

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(
      operatorIdentifier,
      _ => new ProjectionOpExec(attributes, operatorSchemaInfo)
    )
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
    Preconditions.checkArgument(attributes.nonEmpty)

    Schema.newBuilder
      .add(
        attributes
          .map(attribute =>
            new Attribute(
              attribute.getAlias(),
              schemas(0).getAttribute(attribute.getOriginalAttribute()).getType
            )
          )
          .asJava
      )
      .build()
  }
}
