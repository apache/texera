package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLinearSVMOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.svm import LinearSVC"
  modelUserFriendlyName = "Linear Support Vector Machine"
}
