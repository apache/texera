package edu.uci.ics.amber.operator.sklearn

class SklearnLinearSVMOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.svm import LinearSVC"
  override def getUserFriendlyModelName = "Linear Support Vector Machine"
}
