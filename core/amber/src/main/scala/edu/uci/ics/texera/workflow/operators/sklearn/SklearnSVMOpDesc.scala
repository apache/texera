package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSVMOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.svm import SVC"
  modelUserFriendlyName = "Support Vector Machine"
}
