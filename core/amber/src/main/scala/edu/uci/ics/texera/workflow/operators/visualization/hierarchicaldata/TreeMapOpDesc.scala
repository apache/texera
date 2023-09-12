package edu.uci.ics.texera.workflow.operators.visualization.hierarchicaldata

class TreeMapOpDesc extends HierarchicalDataVizOperator {
  override protected def userFriendlyName: String = "TreeMap"
  override protected def operatorDescription: String = "Visualize data in a tree hierarchy"
  override protected def plotlyExpressApiName: String = "treemap"
}
