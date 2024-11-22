package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnPerceptronOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.linear_model import Perceptron"
  override def getUserFriendlyModelName = "Linear Perceptron"
}
