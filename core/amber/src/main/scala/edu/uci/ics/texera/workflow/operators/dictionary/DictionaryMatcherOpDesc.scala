package edu.uci.ics.texera.workflow.operators.dictionary

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

/**
  * Dictionary matcher operator matches a tuple if the specified column is in the given dictionary.
  * It outputs an extra column to label the tuple if it is matched or not
  * This is the description of the operator
  */
class DictionaryMatcherOpDesc extends MapOpDesc {
  @JsonProperty(value = "Dictionary", required = true)
  @JsonPropertyDescription("dictionary values separated by a comma") var dictionary: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column name to match")
  @AutofillAttributeName var attribute: String = _

  @JsonProperty(value = "result attribute", required = true, defaultValue = "matched")
  @JsonPropertyDescription("column name of the matching result") var resultAttribute: String = _

  @JsonProperty(value = "Matching type", required = true) var matchingType: MatchingType = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _, _) => new DictionaryMatcherOpExec(this))
      )
      .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
      .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Dictionary matcher",
      "Matches tuples if they appear in a given dictionary",
      OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    if (resultAttribute == null || resultAttribute.trim.isEmpty) return null
    Schema.newBuilder.add(schemas(0)).add(resultAttribute, AttributeType.BOOLEAN).build
  }
}
