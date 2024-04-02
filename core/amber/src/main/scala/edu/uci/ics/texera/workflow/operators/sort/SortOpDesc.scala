package edu.uci.ics.texera.workflow.operators.sort

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class SortOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonPropertyDescription("column to perform sorting on")
  var attributes: List[SortCriteriaUnit] = _

  override def generatePythonCode(): String = {
    val criteriaString: String = "[" + attributes.map { criteria =>
      s"(\"${criteria.attributeName}\", ${
        criteria.sortPreference match {
          case SortPreference.ASC => "\"ASC\""
          case SortPreference.DESC=> "\"DESC\""
        }
      })"
    }.mkString(", ") + "]"

    s"""from pytexera import *
       |import pandas as pd
       |from datetime import datetime
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        sort_criteria = $criteriaString
       |        df = pd.DataFrame(table)
       |        sort_columns = [attribute for attribute, order in sort_criteria]
       |        ascending_orders = [order == 'ASC' for _, order in sort_criteria]
       |
       |        sorted_df = df.sort_values(by=sort_columns, ascending=ascending_orders)
       |        yield sorted_df""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort",
      "Sort based on the columns and sorting methods",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
