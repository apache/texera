package edu.uci.ics.amber.operator.sklearn

class SklearnSVMOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.svm import SVC"
  override def getUserFriendlyModelName = "Support Vector Machine"
}
