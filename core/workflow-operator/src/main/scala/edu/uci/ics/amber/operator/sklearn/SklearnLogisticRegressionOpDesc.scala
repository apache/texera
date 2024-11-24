package edu.uci.ics.amber.operator.sklearn

class SklearnLogisticRegressionOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.linear_model import LogisticRegression"
  override def getUserFriendlyModelName = "Logistic Regression"
}
