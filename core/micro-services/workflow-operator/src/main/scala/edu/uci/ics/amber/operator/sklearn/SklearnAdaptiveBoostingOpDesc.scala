package edu.uci.ics.amber.operator.sklearn

class SklearnAdaptiveBoostingOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.ensemble import AdaBoostClassifier"
  override def getUserFriendlyModelName = "Adaptive Boosting"
}
