package edu.uci.ics.texera.workflow.operators.visualization.pieChart

import com.fasterxml.jackson.annotation.JsonProperty
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.InputPort
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo
import edu.uci.ics.texera.workflow.common.metadata.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator
import edu.uci.ics.texera.workflow.common.operators.aggregate.DistributedAggregation
import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer

/**
  * PieChart is a visualization operator that can be used to get tuples for pie chart.
  * PieChart returns tuples with name of data (String) and a number (the input can be int, double or String number,
  * but the output will be Double).
  * Note here we assume every name has exactly one data.
  * @author Mingji Han, Xiaozhen Liu
  */
class PieChartOpDesc extends VisualizationOperator {
  @JsonProperty(value = "name column", required = true)
  @AutofillAttributeName var nameColumn: String = _

  @JsonProperty(value = "data column")
  @AutofillAttributeName var dataColumn: String = _

  @JsonProperty(value = "prune ratio", required = true)
  var pruneRatio = .0

  @JsonProperty(value = "chart style", required = true)
  var pieChartEnum: PieChartEnum = _

  override def chartType: String = pieChartEnum.getChartStyle
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    if (nameColumn == null) throw new RuntimeException("pie chart: name column is null")
    if (pruneRatio < 0 || pruneRatio > 1)
      throw new RuntimeException("pie chart: prune ratio not within in [0,1]")

    new PieChartOpExecConfig(
      this.operatorIdentifier,
      Constants.currentWorkerNum,
      nameColumn,
      dataColumn,
      pruneRatio,
      operatorSchemaInfo
    )
  }
  override def operatorInfo =
    OperatorInfo(
      "Pie Chart",
      "View the result in pie chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    val dataType = schemas(0).getAttribute(dataColumn).getType
    if (
      (dataType ne AttributeType.DOUBLE) && (dataType ne AttributeType.INTEGER) && (dataType ne AttributeType.STRING)
    ) throw new RuntimeException("pie chart: data must be number")
    Schema.newBuilder
      .add(
        schemas(0).getAttribute(nameColumn),
        new Attribute(schemas(0).getAttribute(dataColumn).getName, AttributeType.DOUBLE)
      )
      .build
  }
}
