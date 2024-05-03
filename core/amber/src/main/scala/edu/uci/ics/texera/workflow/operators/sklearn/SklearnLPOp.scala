package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLPOp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import Perceptron"
  model = "Perceptron()"
  operatorName = "Linear Perceptron"
}
