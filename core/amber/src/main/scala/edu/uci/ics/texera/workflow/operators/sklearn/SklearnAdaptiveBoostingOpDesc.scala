package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnAdaptiveBoostingOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.ensemble import AdaBoostClassifier"
  override def getUserFriendlyModelName = "Adaptive Boosting"
}
