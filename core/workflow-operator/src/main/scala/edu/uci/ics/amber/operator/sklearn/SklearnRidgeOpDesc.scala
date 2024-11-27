package edu.uci.ics.amber.operator.sklearn

class SklearnRidgeOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.linear_model import RidgeClassifier"
  override def getUserFriendlyModelName = "Ridge Regression"
}
