package edu.uci.ics.texera.workflow.operators.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{ OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.{NewInputPort, NewOutputPort}

class RegexOpDesc extends FilterOpDesc {

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to search regex on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "regex", required = true)
  @JsonPropertyDescription("regular expression")
  var regex: String = _

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Case Insensitive")
  @JsonPropertyDescription("regex match is case sensitive")
  var caseInsensitive: Boolean = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    PhysicalOp.oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecInitInfo((_, _, _) => new RegexOpExec(this))
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Regular Expression",
      operatorDescription = "Search a regular expression in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(NewInputPort.default),
      outputPorts = List(NewOutputPort.default),
      supportReconfiguration = true
    )
}
