package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnKNNOp extends SklearnMLOp {
  modelImport = "from sklearn.neighbors import KNeighborsClassifier"
  model = "KNeighborsClassifier()"
  operatorName = "K-Nearest Neighbors"
}
