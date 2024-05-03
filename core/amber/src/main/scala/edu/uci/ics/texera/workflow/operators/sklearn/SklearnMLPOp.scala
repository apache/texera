package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnMLPOp extends SklearnMLOp {
  model = "from sklearn.neural_network import MLPClassifier"
  name = "Multi-layer Perceptron"
}
