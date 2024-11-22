package edu.uci.ics.amber.operator.sklearn

class SklearnLogisticRegressionCVOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.linear_model import LogisticRegressionCV"
  override def getUserFriendlyModelName = "Logistic Regression Cross Validation"
}
