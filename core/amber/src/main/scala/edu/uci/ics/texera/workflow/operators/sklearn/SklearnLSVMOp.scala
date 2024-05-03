package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLSVMOp extends SklearnMLOp {
  modelImport = "from sklearn.svm import LinearSVC"
  model = "LinearSVC()"
  operatorName = "Linear Support Vector Machine"
}
