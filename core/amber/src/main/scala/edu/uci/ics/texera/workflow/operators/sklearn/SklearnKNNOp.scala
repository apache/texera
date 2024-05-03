package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnKNNOp extends SklearnOp {
  override val modelImport = "from sklearn.neighbors import KNeighborsClassifier"
  override val model = "KNeighborsClassifier()"
  override val operatorName = "K-Nearest Neighbors"
}
