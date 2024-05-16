package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLogisticRegressionOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.linear_model import LogisticRegression"
  override def getUserFriendlyModelName = "Logistic Regression"
}
