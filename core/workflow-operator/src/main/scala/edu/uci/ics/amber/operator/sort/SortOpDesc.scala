package edu.uci.ics.amber.operator.sort

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
class SortOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonPropertyDescription("column to perform sorting on")
  var attributes: List[SortCriteriaUnit] = _

  override def generatePythonCode(): String = {
    val attributeName = "[" + attributes
      .map { criteria =>
        s""""${criteria.attributeName}""""
      }
      .mkString(", ") + "]"
    val sortOrders: String = "[" + attributes
      .map { criteria =>
        criteria.sortPreference match {
          case SortPreference.ASC  => "True"
          case SortPreference.DESC => "False"
        }
      }
      .mkString(", ") + "]"

    s"""from pytexera import *
       |import pandas as pd
       |from datetime import datetime
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        sort_columns = $attributeName
       |        ascending_orders = $sortOrders
       |
       |        sorted_df = table.sort_values(by=sort_columns, ascending=ascending_orders)
       |        yield sorted_df""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort",
      "Sort based on the columns and sorting methods",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
