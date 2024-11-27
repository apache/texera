package edu.uci.ics.amber.operator.sklearn

class SklearnBaggingOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.ensemble import BaggingClassifier"
  override def getUserFriendlyModelName = "Bagging"
}
