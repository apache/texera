package edu.uci.ics.amber.operator.llm

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.UIWidget

class ChatOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Question")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var question: String = _

  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |from core.util.llm import *
       |import pandas as pd
       |from datetime import datetime
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |
       |        # Concatenate all values in 'answer' with '. ' as a separator
       |        context = table['answer']
       |        answer = ask_model(context, '${question}')
       |        yield {'answer': answer}""".stripMargin
  }

  override def getOutputSchemas(inputSchemas: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] = {
    Map(operatorInfo.outputPorts.head.id -> inputSchemas.values.head)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Chat",
      "Chat with LLM",
      OperatorGroupConstants.LLM_GROUP,
      inputPorts = List(InputPort(PortIdentity(0), allowMultiLinks = true)),
      outputPorts = List(OutputPort())
    )
}
