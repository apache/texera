package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSDGOp extends SklearnMLOpDesc {
  model = "from sklearn.linear_model import SGDClassifier"
  name = "Stochastic Gradient Descent"
}
