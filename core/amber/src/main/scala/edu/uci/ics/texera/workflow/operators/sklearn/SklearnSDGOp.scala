package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSDGOp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import SGDClassifier"
  model = "SGDClassifier()"
  operatorName = "Stochastic Gradient Descent"
}
