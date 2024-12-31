package edu.uci.ics.amber.operator.cartesianProduct

import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

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
        OpExecWithClassName("edu.uci.ics.amber.operator.cartesianProduct.CartesianProductOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {

          // Combines the left and right input schemas into a single output schema.
          //
          // - The output schema includes all attributes from the left schema first, followed by
          //   attributes from the right schema.
          // - Duplicate attribute names are resolved by appending an increasing suffix (e.g., `#@1`, `#@2`).
          // - Attributes from the left schema retain their original names in the output schema.
          //
          // Example:
          // Left schema: (dup, dup#@1, dup#@2)
          // Right schema: (r1, r2, dup)
          // Output schema: (dup, dup#@1, dup#@2, r1, r2, dup#@3)
          //
          // In this example, the last attribute from the right schema (`dup`) is renamed to `dup#@3`
          // to avoid conflicts.

          // Initialize a schema builder
          val builder = Schema.builder()

          // Retrieve the left and right schemas
          val leftSchema = inputSchemas(operatorInfo.inputPorts.head.id)
          val rightSchema = inputSchemas(operatorInfo.inputPorts.last.id)

          // Extract attribute names for conflict resolution
          val leftAttributeNames = leftSchema.getAttributeNames.toSet
          val outputAttributeNames = scala.collection.mutable.Set[String]() ++ leftAttributeNames

          // Add all attributes from the left schema to the builder
          builder.add(leftSchema)

          // Process attributes from the right schema
          rightSchema.getAttributes.foreach { attr =>
            var newName = attr.getName

            // Resolve duplicate names by appending a suffix
            while (outputAttributeNames.contains(newName)) {
              val suffixIndex =
                """#@(\d+)$""".r.findFirstMatchIn(newName).map(_.group(1).toInt + 1).getOrElse(1)
              newName = s"${attr.getName}#@$suffixIndex"
            }

            // Add the resolved attribute to the builder
            builder.add(new Attribute(newName, attr.getType))
            outputAttributeNames.add(newName)
          }

          // Build the output schema and associate it with the output port
          val outputSchema = builder.build()
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
      // TODO : refactor to parallelize this operator for better performance and scalability:
      //  can consider hash partition on larger input, broadcast smaller table to each partition
      .withParallelizable(false)

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
}
