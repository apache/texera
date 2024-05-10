package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnMultiLayerPerceptronOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.neural_network import MLPClassifier"
  modelUserFriendlyName = "Multi-layer Perceptron"
}
