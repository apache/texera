package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSVMOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.svm import SVC"
  override def getUserFriendlyModelName = "Support Vector Machine"
}
