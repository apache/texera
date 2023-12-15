package edu.uci.ics.texera.workflow.operators.cartesianProduct

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

class CartesianProductOpDesc extends LogicalOp {
  override def getPhysicalOp(
      executionId: Long,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        executionId,
        operatorIdentifier,
        OpExecInitInfo(_ => new CartesianProductOpExec(operatorSchemaInfo))
      )
      .copy(
        inputPorts = operatorInfo.inputPorts,
        outputPorts = operatorInfo.outputPorts,
        // uses just one worker for now, reads all elements from "left" input port 0 first
        /*
          TODO : refactor to parallelize this operator for better performance and scalability
           - can consider hash partition on larger input, broadcast smaller table to each partition
         */
        numWorkers = 1,
        blockingInputs = List(0),
        dependency = Map(1 -> 0)
      )
  }

  /*
    returns a Schema in order of the left input attributes followed by the right attributes
    duplicate attribute names are handled with an increasing suffix count

    Left schema attributes should always retain the same name in output schema

    For example, Left(dup, dup#@1, dup#@2) cartesian product with Right(r1, r2, dup)
    has output schema: (dup, dup#@1, dup#@2, r1, r2, dup#@3)

    Since the last attribute of Right is a duplicate, it increases suffix until it is
    no longer a duplicate, resulting in dup#@3
   */
  def getOutputSchemaInternal(schemas: Array[Schema]): Schema = {
    // ensure there are exactly two input port schemas to consider
    Preconditions.checkArgument(schemas.length == 2)

    // merge left / right schemas together, sequentially with left schema first
    val builder = Schema.newBuilder()
    val leftSchema = schemas(0)
    val rightSchema = schemas(1)
    builder.add(leftSchema)
    rightSchema.getAttributes.forEach(attr => {
      var attributeName: String = attr.getName
      // append numerical suffix in case of duplicate attributes
      var suffix: Int = 0
      while (builder.build().containsAttribute(attributeName)) {
        suffix += 1
        attributeName = s"${attr.getName}#@$suffix"
      }
      if (suffix == 0) {
        // non-duplicate attribute, add to builder as is
        builder.add(attr)
      } else {
        // renamed the duplicate attribute, construct new Attribute
        builder.add(new Attribute(attributeName, attr.getType))
      }
    })
    builder.build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Cartesian Product",
      "Append fields together to get the cartesian product of two inputs",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  // remove duplicates in attribute names
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    getOutputSchemaInternal(schemas)
  }
}
