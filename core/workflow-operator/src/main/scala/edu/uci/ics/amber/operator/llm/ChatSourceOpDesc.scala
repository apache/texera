package edu.uci.ics.amber.operator.llm

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.annotations.UIWidget
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor

class ChatSourceOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Question")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var question: String = _

  private def generatePythonCode(): String = {
    s"""from pytexera import *
       |from core.util.llm import *
       |import pandas as pd
       |from datetime import datetime
       |
       |class GenerateOperator(UDFSourceOperator):
       |
       |    @overrides
       |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
       |
       |        answer = ask_model('', '${question}')
       |
       |        result = {'answer': answer}
       |        yield result""".stripMargin
  }

  override def getPhysicalOp(
                              workflowId: WorkflowIdentity,
                              executionId: ExecutionIdentity
                            ): PhysicalOp = {
    val physicalOp = PhysicalOp
      .sourcePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecWithCode(generatePythonCode(), "python"))
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
      .withLocationPreference(Option.empty)
      physicalOp.withParallelizable(false)

  }

  override def sourceSchema(): Schema = {
    Schema.apply(List(new Attribute("answer", AttributeType.STRING)))
  }

  override def operatorInfo: OperatorInfo = OperatorInfo(
    "ChatSource",
    "Chat with LLM",
    OperatorGroupConstants.LLM_GROUP,
    inputPorts = List.empty,
    outputPorts = List(OutputPort())
  )
}
