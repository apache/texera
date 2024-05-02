package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnKNNOp extends SklearnOp {
  override def model: String = s"""from sklearn.neighbors import KNeighborsClassifier
       |model = KNeighborsClassifier()""".stripMargin
  override def operatorName: String = "K-Nearest Neighbors"
}