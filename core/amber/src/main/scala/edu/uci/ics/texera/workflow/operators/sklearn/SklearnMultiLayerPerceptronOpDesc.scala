package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnMultiLayerPerceptronOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.neural_network import MLPClassifier"
  override def getUserFriendlyModelName = "Multi-layer Perceptron"
}
