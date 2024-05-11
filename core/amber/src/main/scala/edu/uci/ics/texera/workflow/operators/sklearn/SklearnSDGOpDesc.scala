package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSDGOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.linear_model import SGDClassifier"
  override def getUserFriendlyModelName = "Stochastic Gradient Descent"
}
