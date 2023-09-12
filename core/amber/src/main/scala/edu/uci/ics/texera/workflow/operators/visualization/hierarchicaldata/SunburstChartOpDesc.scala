package edu.uci.ics.texera.workflow.operators.visualization.hierarchicaldata

class SunburstChartOpDesc extends HierarchicalDataVizOperator {
  override protected def userFriendlyName: String = "SunburstChart"
  override protected def operatorDescription: String = "Visualize hierarchical data in as sectors laid out over several levels of concentric rings"
  override protected def plotlyExpressApiName: String = "sunburst"
}
