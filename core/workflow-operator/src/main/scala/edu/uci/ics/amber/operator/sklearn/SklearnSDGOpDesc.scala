package edu.uci.ics.amber.operator.sklearn

class SklearnSDGOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.linear_model import SGDClassifier"
  override def getUserFriendlyModelName = "Stochastic Gradient Descent"
}
