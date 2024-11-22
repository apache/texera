package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnAdaptiveBoostingOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.ensemble import AdaBoostClassifier"
  override def getUserFriendlyModelName = "Adaptive Boosting"
}
