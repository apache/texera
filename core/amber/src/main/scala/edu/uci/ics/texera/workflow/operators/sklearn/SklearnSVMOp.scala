package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSVMOp extends SklearnMLOp {
  modelImport = "from sklearn.svm import SVC"
  model = "SVC()"
  operatorName = "Support Vector Machine"
}
