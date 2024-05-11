package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRidgeOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.linear_model import RidgeClassifier"
  override def getUserFriendlyModelName = "Ridge Regression"
}
