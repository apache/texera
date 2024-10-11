package edu.uci.ics.texera.workflow.operators.cartesianProduct

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.model.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, Schema}
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp

class CartesianProductOpDesc extends LogicalOp {
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new CartesianProductOpExec())
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(
            operatorInfo.outputPorts.head.id -> getOutputSchema(
              Array(
                inputSchemas(operatorInfo.inputPorts.head.id),
                inputSchemas(operatorInfo.inputPorts.last.id)
              )
            )
          )
        )
      )
      // TODO : refactor to parallelize this operator for better performance and scalability:
      //  can consider hash partition on larger input, broadcast smaller table to each partition
      .withParallelizable(false)

  }

  /**
    *    returns a Schema in order of the left input attributes followed by the right attributes
    *    duplicate attribute names are handled with an increasing suffix count
    *
    *    Left schema attributes should always retain the same name in output schema
    *
    *    For example, Left(dup, dup#@1, dup#@2) cartesian product with Right(r1, r2, dup)
    *    has output schema: (dup, dup#@1, dup#@2, r1, r2, dup#@3)
    *
    *    Since the last attribute of Right is a duplicate, it increases suffix until it is
    *    no longer a duplicate, resulting in dup#@3
    */
  def getOutputSchemaInternal(schemas: Array[Schema]): Schema = {
    // merge left / right schemas together, sequentially with left schema first
    val builder = Schema.builder()
    val leftSchema = schemas(0)
    val leftAttributeNames = leftSchema.getAttributeNames
    val rightSchema = schemas(1)
    val rightAttributeNames = rightSchema.getAttributeNames
    builder.add(leftSchema)
    rightSchema.getAttributes.foreach(attr => {
      var newName = attr.getName
      while (
        leftAttributeNames.contains(newName) || rightAttributeNames
          .filterNot(attrName => attrName == attr.getName)
          .contains(newName)
      ) {
        newName = s"$newName#@1"
      }
      if (newName == attr.getName) {
        // non-duplicate attribute, add to builder as is
        builder.add(attr)
      } else {
        // renamed the duplicate attribute, construct new Attribute
        builder.add(new Attribute(newName, attr.getType))
      }
    })
    builder.build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Cartesian Product",
      "Append fields together to get the cartesian product of two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), displayName = "left"),
        InputPort(PortIdentity(1), displayName = "right", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort())
    )

  // remove duplicates in attribute names
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    getOutputSchemaInternal(schemas)
  }
}
