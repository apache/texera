package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnMultiLayerPerceptronOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.neural_network import MLPClassifier"
  override def getUserFriendlyModelName = "Multi-layer Perceptron"
}
