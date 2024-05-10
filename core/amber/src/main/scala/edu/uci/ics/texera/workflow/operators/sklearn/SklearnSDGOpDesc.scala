package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnSDGOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import SGDClassifier"
  modelUserFriendlyName = "Stochastic Gradient Descent"
}
